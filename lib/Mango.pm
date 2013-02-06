package Mango;
use Mojo::Base 'Mojo::EventEmitter';

use Carp 'croak';
use Mango::BSON qw(bson_doc bson_false bson_true);
use Mango::Database;
use Mango::Protocol;
use Mojo::IOLoop;
use Mojo::URL;
use Mojo::Util qw(md5_sum monkey_patch);
use Scalar::Util 'weaken';

use constant DEBUG => $ENV{MANGO_DEBUG} || 0;
use constant DEFAULT_PORT => 27017;

has credentials => sub { [] };
has default_db  => 'admin';
has hosts       => sub { [['localhost']] };
has ioloop      => sub { Mojo::IOLoop->new };
has j           => 0;
has protocol    => sub { Mango::Protocol->new };
has w           => 1;
has wtimeout    => 1000;

our $VERSION = '0.02';

# Operations with reply
for my $name (qw(get_more query)) {
  monkey_patch __PACKAGE__, $name, sub {
    my $self = shift;
    my $cb = ref $_[-1] eq 'CODE' ? pop : undef;
    my ($id, $msg) = $self->_build($name, @_);
    warn "-- Operation $id ($name)\n" if DEBUG;
    $self->_start({id => $id, safe => 1, msg => $msg, cb => $cb});
  };
}

# Operations followed by getLastError
for my $name (qw(delete insert update)) {
  monkey_patch __PACKAGE__, $name, sub {
    my ($self, $ns) = (shift, shift);
    my $cb = ref $_[-1] eq 'CODE' ? pop : undef;

    # Make sure both operations can be written together
    my ($id, $msg) = $self->_build($name, $ns, @_);
    $id = $self->_id;
    $ns =~ s/\..+$/\.\$cmd/;
    my $command = bson_doc
      getLastError => 1,
      j            => $self->j ? bson_true : bson_false,
      w            => $self->w,
      wtimeout     => $self->wtimeout;
    $msg .= $self->protocol->build_query($id, $ns, {}, 0, -1, $command, {});

    warn "-- Operation $id ($name)\n" if DEBUG;
    $self->_start({id => $id, safe => 1, msg => $msg, cb => $cb});
  };
}

sub DESTROY { shift->_cleanup }

sub new {
  my $self = shift->SUPER::new;

  # Protocol
  return $self unless my $string = shift;
  my $url = Mojo::URL->new($string);
  croak qq{Invalid MongoDB connection string "$string"}
    unless $url->protocol eq 'mongodb';

  # Hosts
  my @hosts;
  /^([^,:]+)(?::(\d+))?/ and push @hosts, $2 ? [$1, $2] : [$1]
    for split /,/, join(':', map { $_ // '' } $url->host, $url->port);
  $self->hosts(\@hosts) if @hosts;

  # Database
  if (my $db = $url->path->parts->[0]) { $self->default_db($db) }

  # User and password
  push @{$self->credentials}, [$self->default_db, $1, $2]
    if ($url->userinfo // '') =~ /^([^:]+):([^:]+)$/;

  # Options
  my $query = $url->query;
  if (my $j       = $query->param('journal'))    { $self->j($j) }
  if (my $w       = $query->param('w'))          { $self->w($w) }
  if (my $timeout = $query->param('wtimeoutMS')) { $self->wtimeout($timeout) }

  return $self;
}

sub db {
  my ($self, $name) = @_;
  $name //= $self->default_db;
  return Mango::Database->new(mango => $self, name => $name);
}

sub is_active { !!(scalar @{$_[0]{queue} || []} || $_[0]{current}) }

sub kill_cursors {
  my $self = shift;
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;
  my ($id, $msg) = $self->_build('kill_cursors', @_);
  warn "-- Unsafe operation $id (kill_cursors)\n" if DEBUG;
  $self->_start({id => $id, safe => 0, msg => $msg, cb => $cb});
}

sub _auth {
  my ($self, $credentials, $auth, $err, $reply) = @_;
  my ($db, $user, $pass) = @$auth;

  # No nonce value
  return $self->_connected($credentials) if $err || !$reply->{docs}[0]{ok};
  my $nonce = $reply->{docs}[0]{nonce};

  # Authenticate
  my $key = md5_sum $nonce . $user . md5_sum "${user}:mongo:${pass}";
  my $command
    = bson_doc(authenticate => 1, user => $user, nonce => $nonce, key => $key);
  $self->_command($db, $command, sub { shift->_connected($credentials) });
}

sub _build {
  my ($self, $name) = (shift, shift);
  my $id     = $self->_id;
  my $method = "build_$name";
  return ($id, $self->protocol->$method($id, @_));
}

sub _cleanup {
  my $self = shift;
  return unless my $loop = $self->_loop;

  # Clean up connection
  $loop->remove(delete $self->{connection}) if $self->{connection};

  # Clean up all operations
  my $queue = delete $self->{queue} || [];
  unshift @$queue, $self->{current} if $self->{current};
  $self->_finish(undef, $_->{cb}, 'Premature connection close') for @$queue;
}

sub _close {
  my $self = shift;
  $self->_error;
  $self->_connect;
}

sub _command {
  my ($self, $db, $command, $cb) = @_;

  # Skip the queue and run command right away
  my $id = $self->_id;
  my $msg
    = $self->protocol->build_query($id, "$db.\$cmd", {}, 0, -1, $command, {});
  unshift @{$self->{queue}}, {id => $id, safe => 1, cb => $cb, msg => $msg};
  warn "-- Fast operation $id (query)\n" if DEBUG;
  $self->_write;
}

sub _connect {
  my $self = shift;

  weaken $self;
  my ($host, $port) = @{$self->hosts->[0]};
  $self->{connection} = $self->_loop->client(
    {address => $host, port => $port // DEFAULT_PORT} => sub {
      my ($loop, $err, $stream) = @_;

      # Connection error
      return $self->_error($err) if $err;

      # Connection established
      $stream->timeout(0);
      $stream->on(close => sub { $self->_close });
      $stream->on(error => sub { $self && $self->_error(pop) });
      $stream->on(read  => sub { $self->_read(pop) });
      $self->_connected([@{$self->credentials}]);
    }
  );
}

sub _connected {
  my ($self, $credentials) = @_;

  # No authentication
  return $self->_write unless my $auth = shift @$credentials;

  # Get nonce value and authenticate
  my $cb = sub { shift->_auth($credentials, $auth, @_) };
  $self->_command($auth->[0], {getnonce => 1}, $cb);
}

sub _error {
  my ($self, $err) = @_;
  my $current = delete $self->{current};
  $current //= shift @{$self->{queue}} if $err;
  return $err ? $self->emit(error => $err) : undef unless $current;
  $self->_finish(undef, $current->{cb}, $err || 'Premature connection close');
}

sub _finish {
  my ($self, $reply, $cb, $err) = @_;
  my $docs = $reply ? $reply->{docs} : [];
  $err ||= $docs->[0]{'$err'} if @$docs && $reply->{cursor} == 0;
  $self->$cb($err, $reply);
}

sub _id { $_[0]->{id} = $_[0]->protocol->next_id($_[0]->{id} // 0) }

sub _loop { $_[0]{nb} ? Mojo::IOLoop->singleton : $_[0]->ioloop }

sub _queue {
  my ($self, $op) = @_;
  push @{$self->{queue} ||= []}, $op;
  if   ($self->{connection}) { $self->_write }
  else                       { $self->_connect }
}

sub _read {
  my ($self, $chunk) = @_;

  $self->{buffer} .= $chunk;
  while (my $reply = $self->protocol->parse_reply(\$self->{buffer})) {
    warn "-- Client <<< Server ($reply->{to})\n" if DEBUG;
    next unless $reply->{to} == $self->{current}{id};
    $self->_finish($reply, (delete $self->{current})->{cb});
  }
  $self->_write;
}

sub _start {
  my ($self, $op) = @_;

  # Non-blocking
  if ($op->{cb}) {

    # Start non-blocking
    unless ($self->{nb}) {
      croak 'Blocking operation in progress' if $self->is_active;
      $self->_cleanup;
      $self->{nb}++;
    }
    return $self->_queue($op);
  }

  # Start blocking
  if ($self->{nb}) {
    croak 'Non-blocking operations in progress' if $self->is_active;
    $self->_cleanup;
    delete $self->{nb};
  }
  my ($err, $reply);
  $op->{cb} = sub {
    (my $self, $err, $reply) = @_;
    $self->ioloop->stop;
  };
  $self->_queue($op);

  # Start event loop
  $self->ioloop->start;

  # Throw blocking errors
  croak $err if $err;

  return $reply;
}

sub _write {
  my $self = shift;

  return if $self->{current};
  return unless my $stream = $self->_loop->stream($self->{connection});
  return unless my $current = $self->{current} = shift @{$self->{queue}};

  warn "-- Client >>> Server ($current->{id})\n" if DEBUG;
  $stream->write(delete $current->{msg});

  # Unsafe operations are done when they are written
  return if $current->{safe};
  weaken $self;
  $stream->write(
    '' => sub { $self->_finish(undef, delete($self->{current})->{cb}) });
}

1;

=head1 NAME

Mango - Pure-Perl non-blocking I/O MongoDB client

=head1 SYNOPSIS

  use Mango;
  my $mango = Mango->new('mongodb://localhost:27017');

  # Insert document
  my $oid = $mango->db('test')->collection('foo')->insert({bar => 'baz'});

  # Find document
  my $doc = $mango->db('test')->collection('foo')->find_one({bar => 'baz'});
  say $doc->{bar};

  # Update document
  $mango->db('test')->collection('foo')
    ->update({bar => 'baz'}, {bar => 'yada'});

  # Remove document
  $mango->db('test')->collection('foo')->remove({bar => 'yada'});

  # Insert document with special BSON types
  use Mango::BSON ':bson';
  my $oid = $mango->db('test')->collection('foo')
    ->insert({data => bson_bin("\x00\x01"), now => bson_time});

  # Find documents non-blocking (does work inside a running event loop)
  my $delay = Mojo::IOLoop->delay(sub {
    my ($delay, @docs) = @_;
    ...
  });
  for my $name (qw(foo bar)) {
    $delay->begin;
    $mango->db('test')->collection('users')->find({name => $name})->all(sub {
      my ($cursor, $err, $docs) = @_;
      $delay->end(@$docs);
    });
  }
  $delay->wait unless Mojo::IOLoop->is_running;

=head1 DESCRIPTION

L<Mango> is a pure-Perl non-blocking I/O MongoDB client, optimized for use
with the L<Mojolicious> real-time web framework, and with multiple event loop
support.

Note that this whole distribution is EXPERIMENTAL and will change without
warning!

Many features are still incomplete or missing, so you should wait for a stable
1.0 release before using any of the modules in this distribution in a
production environment. Unsafe operations are not supported, so far this is
considered a feature.

This is a L<Mojolicious> spin-off project, so we follow the
L<same rules|Mojolicious::Guides::Contributing>.

Optional modules L<EV> (4.0+), L<IO::Socket::IP> (0.16+) and
L<IO::Socket::SSL> (1.75+) are supported transparently through
L<Mojo::IOLoop>, and used if installed. Individual features can also be
disabled with the C<MOJO_NO_IPV6> and C<MOJO_NO_TLS> environment variables.

=head1 EVENTS

L<Mango> inherits all events from L<Mojo::EventEmitter> and can emit the
following new ones.

=head2 error

  $mango->on(error => sub {
    my ($mango, $err) = @_;
    ...
  });

Emitted if an error occurs that can't be associated with an operation.

  $mango->on(error => sub {
    my ($mango, $err) = @_;
    say "This looks bad: $err";
  });

=head1 ATTRIBUTES

L<Mango> implements the following attributes.

=head2 credentials

  my $credentials = $mango->credentials;
  $mango          = $mango->credentials([['test', 'sri', 's3cret']]);

Authentication credentials that will be used on every reconnect.

=head2 default_db

  my $name = $mango->default_db;
  $mango   = $mango->default_db('test');

Default database, defaults to C<admin>.

=head2 hosts

  my $hosts = $mango->hosts;
  $mango    = $mango->hosts([['localhost', 3000]]);

Server to connect to, defaults to C<localhost> and port C<27017>.

=head2 ioloop

  my $loop = $mango->ioloop;
  $mango   = $mango->ioloop(Mojo::IOLoop->new);

Event loop object to use for blocking I/O operations, defaults to a
L<Mojo::IOLoop> object.

=head2 j

  my $j  = $mango->j;
  $mango = $mango->j(1);

Wait for all operations to have reached the journal, defaults to C<0>.

=head2 protocol

  my $protocol = $mango->protocol;
  $mango       = $mango->protocol(Mango::Protocol->new);

Protocol handler, defaults to a L<Mango::Protocol> object.

=head2 w

  my $w  = $mango->w;
  $mango = $mango->w(1);

Wait for all operations to have reached at least this many servers, C<1>
indicates just primary, C<2> indicates primary and at least one secondary,
defaults to C<1>.

=head2 wtimeout

  my $timeout = $mango->wtimeout;
  $mango      = $mango->wtimeout(1);

Timeout for write propagation in milliseconds, defaults to C<1000>.

=head1 METHODS

L<Mango> inherits all methods from L<Mojo::Base> and implements the following
new ones.

=head2 new

  my $mango = Mango->new;
  my $mango = Mango->new('mongodb://localhost:3000/mango_test?w=2');

Construct a new L<Mango> object.

=head2 db

  my $db = $mango->db;
  my $db = $mango->db('test');

Get L<Mango::Database> object for database, uses C<default_db> if no name is
provided.

=head2 delete

  my $reply = $mango->delete($name, $flags, $query);

Perform low level C<delete> operation followed by C<getLastError> command. You
can also append a callback to perform operation non-blocking.

  $mango->delete(($name, $flags, $query) => sub {
    my ($mango, $err, $reply) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 get_more

  my $reply = $mango->get_more($name, $limit, $cursor);

Perform low level C<get_more> operation. You can also append a callback to
perform operation non-blocking.

  $mango->get_more(($name, $limit, $cursor) => sub {
    my ($mango, $err, $reply) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 insert

  my $reply = $mango->insert($name, $flags, @docs);

Perform low level C<insert> operation followed by C<getLastError> command. You
can also append a callback to perform operation non-blocking.

  $mango->insert(($name, $flags, @docs) => sub {
    my ($mango, $err, $reply) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 is_active

  my $success = $mango->is_active;

Check if there are still operations in progress.

=head2 kill_cursors

  $mango->kill_cursors(@ids);

Perform low level C<kill_cursors> operation. You can also append a callback to
perform operation non-blocking.

    $mango->kill_cursors(@ids => sub {
      my $mango = shift;
      ...
    });
    Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 query

  my $reply = $mango->query($name, $flags, $skip, $limit, $query, $fields);

Perform low level C<query> operation. You can also append a callback to
perform operation non-blocking.

  $mango->query(($name, $flags, $skip, $limit, $query, $fields) => sub {
    my ($mango, $err, $reply) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 update

  my $reply = $mango->update($name, $flags, $query, $update);

Perform low level C<update> operation followed by C<getLastError> command. You
can also append a callback to perform operation non-blocking.

  $mango->update(($name, $flags, $query, $update) => sub {
    my ($mango, $err, $reply) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head1 DEBUGGING

You can set the C<MANGO_DEBUG> environment variable to get some advanced
diagnostics information printed to C<STDERR>.

  MANGO_DEBUG=1

=head1 SPONSORS

Some of the work on this distribution has been sponsored by an anonymous
donor, thank you!

=head1 AUTHOR

Sebastian Riedel, C<sri@cpan.org>.

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2013, Sebastian Riedel.

This program is free software, you can redistribute it and/or modify it under
the terms of the Artistic License version 2.0.

=head1 SEE ALSO

L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
