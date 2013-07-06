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
has max_connections => 5;
has protocol        => sub { Mango::Protocol->new };
has w               => 1;
has wtimeout        => 1000;

our $VERSION = '0.05';

# Operations with reply
for my $name (qw(get_more query)) {
  monkey_patch __PACKAGE__, $name, sub {
    my $self = shift;
    my $cb = ref $_[-1] eq 'CODE' ? pop : undef;
    my ($next, $msg) = $self->_build($name, @_);
    warn "-- Operation $next ($name)\n" if DEBUG;
    $self->_start({id => $next, safe => 1, msg => $msg, cb => $cb});
  };
}

# Operations followed by getLastError
for my $name (qw(delete insert update)) {
  monkey_patch __PACKAGE__, $name, sub {
    my ($self, $ns) = (shift, shift);
    my $cb = ref $_[-1] eq 'CODE' ? pop : undef;

    # Make sure both operations can be written together
    my ($next, $msg) = $self->_build($name, $ns, @_);
    $next = $self->_id;
    $ns =~ s/\..+$/\.\$cmd/;
    my $command = bson_doc
      getLastError => 1,
      j            => $self->j ? bson_true : bson_false,
      w            => $self->w,
      wtimeout     => $self->wtimeout;
    $msg .= $self->protocol->build_query($next, $ns, {}, 0, -1, $command, {});

    warn "-- Operation $next ($name)\n" if DEBUG;
    $self->_start({id => $next, safe => 1, msg => $msg, cb => $cb});
  };
}

sub DESTROY { shift->_cleanup }

sub new {
  my $self = shift->SUPER::new;

  # Protocol
  return $self unless my $str = shift;
  my $url = Mojo::URL->new($str);
  croak qq{Invalid MongoDB connection string "$str"}
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
  my $db = Mango::Database->new(mango => $self, name => $name);
  weaken $db->{mango};
  return $db;
}

sub is_active {
  my $self = shift;
  return !!(@{$self->{queue} || []}
    || grep { $_->{last} } values %{$self->{connections} || {}});
}

sub kill_cursors {
  my $self = shift;
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;
  my ($next, $msg) = $self->_build('kill_cursors', @_);
  warn "-- Unsafe operation $next (kill_cursors)\n" if DEBUG;
  $self->_start({id => $next, safe => 0, msg => $msg, cb => $cb});
}

sub _auth {
  my ($self, $id, $credentials, $auth, $err, $reply) = @_;
  my ($db, $user, $pass) = @$auth;

  # Run "authenticate" command with "nonce" value
  my $nonce = $reply->{docs}[0]{nonce} // '';
  my $key = md5_sum $nonce . $user . md5_sum "$user:mongo:$pass";
  my $command
    = bson_doc(authenticate => 1, user => $user, nonce => $nonce, key => $key);
  my $cb = sub { shift->_connected($id, $credentials) };
  $self->_fast($id, $db, $command, $cb);
}

sub _build {
  my ($self, $name) = (shift, shift);
  my $next   = $self->_id;
  my $method = "build_$name";
  return ($next, $self->protocol->$method($next, @_));
}

sub _cleanup {
  my $self = shift;
  return unless my $loop = $self->_loop;

  # Clean up connections
  my $connections = delete $self->{connections};
  $loop->remove($_) for keys %$connections;

  # Clean up active operations
  my $queue = delete $self->{queue} || [];
  $_->{last} and unshift @$queue, $_->{last} for values %$connections;
  $self->_finish(undef, $_->{cb}, 'Premature connection close') for @$queue;
}

sub _close {
  my ($self, $id) = @_;
  $self->_error($id);
  $self->_connect if delete $self->{connections}{$id};
}

sub _connect {
  my $self = shift;

  weaken $self;
  my ($host, $port) = @{$self->hosts->[0]};
  my $id;
  $id = $self->_loop->client(
    {address => $host, port => $port // DEFAULT_PORT} => sub {
      my ($loop, $err, $stream) = @_;

      # Connection error
      return $self && $self->_error($id, $err) if $err;

      # Connection established
      $stream->timeout(0);
      $stream->on(close => sub { $self->_close($id) });
      $stream->on(error => sub { $self && $self->_error($id, pop) });
      $stream->on(read => sub { $self->_read($id, pop) });
      $self->_connected($id, [@{$self->credentials}]);
    }
  );
  $self->{connections}{$id} = {start => 1};

  my $num = scalar keys %{$self->{connections}};
  warn "-- New connection ($host:$port:$num)\n" if DEBUG;
}

sub _connected {
  my ($self, $id, $credentials) = @_;

  # No authentication
  return $self->_next unless my $auth = shift @$credentials;

  # Run "getnonce" command followed by "authenticate"
  my $cb = sub { shift->_auth($id, $credentials, $auth, @_) };
  $self->_fast($id, $auth->[0], {getnonce => 1}, $cb);
}

sub _error {
  my ($self, $id, $err) = @_;

  my $c    = delete $self->{connections}{$id};
  my $last = $c->{last};
  $last //= shift @{$self->{queue}} if $err;
  return $err ? $self->emit(error => $err) : undef unless $last;
  $self->_finish(undef, $last->{cb}, $err || 'Premature connection close');
}

sub _fast {
  my ($self, $id, $db, $command, $cb) = @_;

  # Handle errors
  my $protocol = $self->protocol;
  my $wrapper  = sub {
    my ($self, $err, $reply) = @_;
    $err ||= $protocol->command_error($reply);
    return $err ? $self->_error($id, $err) : $self->$cb($err, $reply);
  };

  # Skip the queue and run command right away
  my $next = $self->_id;
  my $msg
    = $protocol->build_query($next, "$db.\$cmd", {}, 0, -1, $command, {});
  $self->{connections}{$id}{fast}
    = {id => $next, safe => 1, msg => $msg, cb => $wrapper};
  warn "-- Fast operation $next (query)\n" if DEBUG;
  $self->_next;
}

sub _finish {
  my ($self, $reply, $cb, $err) = @_;
  $self->$cb($err || $self->protocol->query_failure($reply), $reply);
}

sub _id { $_[0]{id} = $_[0]->protocol->next_id($_[0]{id} // 0) }

sub _loop { $_[0]{nb} ? Mojo::IOLoop->singleton : $_[0]->ioloop }

sub _next {
  my ($self, $op) = @_;

  push @{$self->{queue} ||= []}, $op if $op;

  my @ids = keys %{$self->{connections}};
  my $start;
  $self->_write($_) and $start++ for @ids;
  $self->_connect
    if $op && !$start && @{$self->{queue}} && @ids < $self->max_connections;
}

sub _read {
  my ($self, $id, $chunk) = @_;

  $self->{buffer} .= $chunk;
  my $c = $self->{connections}{$id};
  while (my $reply = $self->protocol->parse_reply(\$self->{buffer})) {
    warn "-- Client <<< Server ($reply->{to})\n" if DEBUG;
    next unless $reply->{to} == $c->{last}{id};
    $self->_finish($reply, (delete $c->{last})->{cb});
  }
  $self->_next;
}

sub _start {
  my ($self, $op) = @_;

  # Non-blocking
  if ($op->{cb}) {

    # Start non-blocking
    unless ($self->{nb}) {
      croak 'Blocking operation in progress' if $self->is_active;
      warn "-- Switching to non-blocking mode\n" if DEBUG;
      $self->_cleanup;
      $self->{nb}++;
    }

    return $self->_next($op);
  }

  # Start blocking
  if ($self->{nb}) {
    croak 'Non-blocking operations in progress' if $self->is_active;
    warn "-- Switching to blocking mode\n" if DEBUG;
    $self->_cleanup;
    delete $self->{nb};
  }

  my ($err, $reply);
  $op->{cb} = sub {
    (my $self, $err, $reply) = @_;
    $self->ioloop->stop;
  };
  $self->_next($op);
  $self->ioloop->start;

  # Throw blocking errors
  croak $err if $err;

  return $reply;
}

sub _write {
  my ($self, $id) = @_;

  my $c = $self->{connections}{$id};
  return $c->{start} if $c->{last};
  return undef       unless my $stream = $self->_loop->stream($id);
  delete $c->{start} unless my $last   = delete $c->{fast};
  return $c->{start} unless $c->{last} = $last ||= shift @{$self->{queue}};
  warn "-- Client >>> Server ($last->{id})\n" if DEBUG;
  $stream->write(delete $last->{msg});

  # Unsafe operations are done when they are written
  return $c->{start} if $last->{safe};
  weaken $self;
  $stream->write('', sub { $self->_finish(undef, delete($c->{last})->{cb}) });
  return $c->{start};
}

1;

=encoding utf8

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

  # Blocking parallel find (does not work inside a running event loop)
  my $delay = Mojo::IOLoop->delay;
  for my $name (qw(sri marty)) {
    my $end = $delay->begin(0);
    $mango->db('test')->collection('users')->find({name => $name})->all(sub {
      my ($cursor, $err, $docs) = @_;
      $end->(@$docs);
    });
  }
  my @docs = $delay->wait;

  # Non-blocking parallel find (does work inside a running event loop)
  my $delay = Mojo::IOLoop->delay(sub {
    my ($delay, @docs) = @_;
    ...
  });
  for my $name (qw(sri marty)) {
    my $end = $delay->begin(0);
    $mango->db('test')->collection('users')->find({name => $name})->all(sub {
      my ($cursor, $err, $docs) = @_;
      $end->(@$docs);
    });
  }
  $delay->wait unless Mojo::IOLoop->is_running;

=head1 DESCRIPTION

L<Mango> is a pure-Perl non-blocking I/O MongoDB client, optimized for use
with the L<Mojolicious> real-time web framework, and with multiple event loop
support.

To learn more about MongoDB you should take a look at the
L<official documentation|http://docs.mongodb.org>.

Note that this whole distribution is EXPERIMENTAL and will change without
warning!

Many features are still incomplete or missing, so you should wait for a stable
1.0 release before using any of the modules in this distribution in a
production environment. Unsafe operations are not supported, so far this is
considered a feature.

For better scalability (epoll, kqueue) and to provide IPv6 as well as TLS
support, the optional modules L<EV> (4.0+), L<IO::Socket::IP> (0.16+) and
L<IO::Socket::SSL> (1.75+) will be used automatically by L<Mojo::IOLoop> if
they are installed. Individual features can also be disabled with the
MOJO_NO_IPV6 and MOJO_NO_TLS environment variables.

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

=head2 max_connections

  my $max = $mango->max_connections;
  $mango  = $mango->max_connections(5);

Maximum number of connections to use for non-blocking operations, defaults to
C<5>.

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

  my $reply = $mango->get_more($name, $return, $cursor);

Perform low level C<get_more> operation. You can also append a callback to
perform operation non-blocking.

  $mango->get_more(($name, $return, $cursor) => sub {
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
      my ($mango, $err) = @_;
      ...
    });
    Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 query

  my $reply = $mango->query($name, $flags, $skip, $return, $query, $fields);

Perform low level C<query> operation. You can also append a callback to
perform operation non-blocking.

  $mango->query(($name, $flags, $skip, $return, $query, $fields) => sub {
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

Some of the work on this distribution has been sponsored by
L<Drip Depot|http://www.dripdepot.com>, thank you!

=head1 AUTHOR

Sebastian Riedel, C<sri@cpan.org>.

=head1 CREDITS

In alphabetical order:

=over 2

Andrey Khozov

=back

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2013, Sebastian Riedel.

This program is free software, you can redistribute it and/or modify it under
the terms of the Artistic License version 2.0.

=head1 SEE ALSO

L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
