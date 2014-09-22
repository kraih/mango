package Mango;
use Mojo::Base 'Mojo::EventEmitter';

use Carp 'croak';
use Mango::BSON 'bson_doc';
use Mango::Database;
use Mango::Protocol;
use Mojo::IOLoop;
use Mojo::URL;
use Mojo::Util qw(dumper md5_sum);
use Scalar::Util 'weaken';

use constant DEBUG => $ENV{MANGO_DEBUG} || 0;
use constant DEFAULT_PORT => 27017;

has credentials => sub { [] };
has default_db  => 'admin';
has hosts       => sub { [['localhost']] };
has [qw(inactivity_timeout j)] => 0;
has ioloop => sub { Mojo::IOLoop->new };
has max_bson_size   => 16777216;
has max_connections => 5;
has [qw(max_write_batch_size wtimeout)] => 1000;
has protocol => sub { Mango::Protocol->new };
has w => 1;

our $VERSION = '1.15';

sub DESTROY { shift->_cleanup }

sub backlog { scalar @{shift->{queue} || []} }

sub db {
  my ($self, $name) = @_;
  $name //= $self->default_db;
  my $db = Mango::Database->new(mango => $self, name => $name);
  weaken $db->{mango};
  return $db;
}

sub from_string {
  my ($self, $str) = @_;

  # Protocol
  return $self unless $str;
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

sub get_more { shift->_op('get_more', 1, @_) }

sub kill_cursors { shift->_op('kill_cursors', 0, @_) }

sub new { shift->SUPER::new->from_string(@_) }

sub query { shift->_op('query', 1, @_) }

sub _auth {
  my ($self, $id) = @_;

  # No more authentication (connection is ready)
  return $self->emit(connection => $id)->_next
    unless my $auth = shift @{$self->{connections}{$id}{credentials}};

  # Run "getnonce" command followed by "authenticate"
  my $cb = sub { shift->_nonce($id, $auth, @_) };
  $self->_fast($id, $auth->[0], {getnonce => 1}, $cb);
}

sub _build {
  my ($self, $name) = (shift, shift);
  my $next = $self->_id;
  warn "-- Operation #$next ($name)\n@{[dumper [@_]]}" if DEBUG;
  my $method = "build_$name";
  return ($next, $self->protocol->$method($next, @_));
}

sub _cleanup {
  my $self = shift;
  return unless $self->_loop(0);

  # Clean up connections
  delete $self->{pid};
  my $connections = delete $self->{connections};
  $self->_loop($connections->{$_}{nb})->remove($_) for keys %$connections;

  # Clean up active operations
  my $queue = delete $self->{queue} || [];
  $_->{last} && !$_->{start} && unshift @$queue, $_->{last}
    for values %$connections;
  $self->_finish(undef, $_->{cb}, 'Premature connection close') for @$queue;
}

sub _close {
  my ($self, $id) = @_;

  return unless my $c = delete $self->{connections}{$id};
  my $last = $c->{last};
  $self->_finish(undef, $last->{cb}, 'Premature connection close') if $last;
  $self->_connect($c->{nb}) if @{$self->{queue}};
}

sub _connect {
  my ($self, $nb, $hosts) = @_;

  my ($host, $port) = @{shift @{$hosts ||= [@{$self->hosts}]}};
  weaken $self;
  my $id;
  $id = $self->_loop($nb)->client(
    {address => $host, port => $port //= DEFAULT_PORT} => sub {
      my ($loop, $err, $stream) = @_;

      # Connection error (try next server)
      if ($err) {
        return $self->_error($id, $err) unless @$hosts;
        delete $self->{connections}{$id};
        return $self->_connect($nb, $hosts);
      }

      # Connection established
      $stream->timeout($self->inactivity_timeout);
      $stream->on(close => sub { $self && $self->_close($id) });
      $stream->on(error => sub { $self && $self->_error($id, pop) });
      $stream->on(read => sub { $self->_read($id, pop) });

      # Check node information with "isMaster" command
      my $cb = sub { shift->_master($id, $nb, $hosts, pop) };
      $self->_fast($id, $self->default_db, {isMaster => 1}, $cb);
    }
  );
  $self->{connections}{$id}
    = {credentials => [@{$self->credentials}], nb => $nb, start => 1};

  my $num = scalar keys %{$self->{connections}};
  warn "-- New connection ($host:$port:$num)\n" if DEBUG;
}

sub _error {
  my ($self, $id, $err) = @_;

  return unless my $c = delete $self->{connections}{$id};
  $self->_loop($c->{nb})->remove($id);

  my $last = $c->{last} // shift @{$self->{queue}};
  $self->_finish(undef, $last->{cb}, $err) if $last;
}

sub _fast {
  my ($self, $id, $db, $command, $cb) = @_;

  # Handle errors
  my $wrapper = sub {
    my ($self, $err, $reply) = @_;

    my $doc = $reply->{docs}[0];
    $err ||= $self->protocol->command_error($doc);
    return $self->$cb(undef, $doc) unless $err;

    return unless my $last = shift @{$self->{queue}};
    $self->_finish(undef, $last->{cb}, $err);
  };

  # Skip the queue and run command right away
  my ($next, $msg)
    = $self->_build('query', "$db.\$cmd", {}, 0, -1, $command, {});
  $self->{connections}{$id}{fast}
    = {id => $next, safe => 1, msg => $msg, cb => $wrapper};
  $self->_next;
}

sub _finish {
  my ($self, $reply, $cb, $err) = @_;
  $self->$cb($err || $self->protocol->query_failure($reply), $reply);
}

sub _id { $_[0]{id} = $_[0]->protocol->next_id($_[0]{id} // 0) }

sub _loop { $_[1] ? Mojo::IOLoop->singleton : $_[0]->ioloop }

sub _master {
  my ($self, $id, $nb, $hosts, $doc) = @_;

  # Check version
  return $self->_error($id, 'MongoDB version 2.6 required')
    unless ($doc->{maxWireVersion} || 0) >= 2;

  # Continue with authentication if we are connected to the primary
  return $self->_auth($id) if $doc->{ismaster};

  # Get primary and try to connect again
  unshift @$hosts, [$1, $2] if ($doc->{primary} // '') =~ /^(.+):(\d+)$/;
  return $self->_error($id, "Couldn't find primary node") unless @$hosts;
  delete $self->{connections}{$id};
  $self->_loop($nb)->remove($id);
  $self->_connect($nb, $hosts);
}

sub _next {
  my ($self, $op) = @_;

  # Make sure all connections are saturated
  push @{$self->{queue} ||= []}, $op if $op;
  my $connections = $self->{connections};
  my $start;
  $self->_write($_) and $start++ for my @ids = keys %$connections;

  # Check if we need a blocking connection
  return unless $op;
  return $self->_connect(0)
    if !$op->{nb} && !grep { !$connections->{$_}{nb} } @ids;

  # Check if we need more non-blocking connections
  $self->_connect(1)
    if !$start && @{$self->{queue}} && @ids < $self->max_connections;
}

sub _nonce {
  my ($self, $id, $auth, $err, $doc) = @_;

  # Run "authenticate" command with "nonce" value
  my $nonce = $doc->{nonce} // '';
  my ($db, $user, $pass) = @$auth;
  my $key = md5_sum $nonce . $user . md5_sum "$user:mongo:$pass";
  my $command
    = bson_doc(authenticate => 1, user => $user, nonce => $nonce, key => $key);
  $self->_fast($id, $db, $command, sub { shift->_auth($id) });
}

sub _op {
  my ($self, $op, $safe) = (shift, shift, shift);
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;
  my ($next, $msg) = $self->_build($op, @_);
  $self->_start(
    {id => $next, safe => $safe, msg => $msg, nb => !!$cb, cb => $cb});
}

sub _read {
  my ($self, $id, $chunk) = @_;

  my $c = $self->{connections}{$id};
  $c->{buffer} .= $chunk;
  while (my $reply = $self->protocol->parse_reply(\$c->{buffer})) {
    warn "-- Client <<< Server (#$reply->{to})\n@{[dumper $reply]}" if DEBUG;
    next unless $reply->{to} == $c->{last}{id};
    $self->_finish($reply, (delete $c->{last})->{cb});
  }
  $self->_next;
}

sub _start {
  my ($self, $op) = @_;

  # Fork safety
  $self->_cleanup unless ($self->{pid} //= $$) eq $$;

  # Non-blocking
  return $self->_next($op) if $op->{cb};

  # Blocking
  my ($err, $reply);
  $op->{cb} = sub { shift->ioloop->stop; ($err, $reply) = @_ };
  $self->_next($op);
  $self->ioloop->start;
  return $err ? croak $err : $reply;
}

sub _write {
  my ($self, $id) = @_;

  # Make sure connection has not been corrupted while event loop was stopped
  my $c = $self->{connections}{$id};
  return $c->{start} if $c->{last};
  my $loop = $self->_loop($c->{nb});
  return undef unless my $stream = $loop->stream($id);
  if (!$loop->is_running && $stream->is_readable) {
    $stream->close;
    return undef;
  }

  # Fast operation
  delete $c->{start} unless my $last = delete $c->{fast};

  # Blocking operations have a higher precedence
  return $c->{start}
    unless $last || ($c->{nb} xor !($self->{queue}->[-1] || {})->{nb});
  $last ||= $c->{nb} ? shift @{$self->{queue}} : pop @{$self->{queue}};

  return $c->{start} unless $c->{last} = $last;
  warn "-- Client >>> Server (#$last->{id})\n" if DEBUG;
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

Mango - Pure-Perl non-blocking I/O MongoDB driver

=head1 SYNOPSIS

  use Mango;

  # Insert document
  my $mango = Mango->new('mongodb://localhost:27017');
  my $oid   = $mango->db('test')->collection('foo')->insert({bar => 'baz'});

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

  # Non-blocking concurrent find
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
  $delay->wait;

  # Event loops such as AnyEvent are supported through EV
  use EV;
  use AnyEvent;
  my $cv = AE::cv;
  $mango->db('test')->command(buildInfo => sub {
    my ($db, $err, $doc) = @_;
    $cv->send($doc->{version});
  });
  say $cv->recv;

=head1 DESCRIPTION

L<Mango> is a pure-Perl non-blocking I/O MongoDB driver, optimized for use
with the L<Mojolicious> real-time web framework, and with multiple event loop
support. Since MongoDB is still changing rapidly, only the latest stable
version is supported.

To learn more about MongoDB you should take a look at the
L<official documentation|http://docs.mongodb.org>, the documentation included
in this distribution is no replacement for it.

Many arguments passed to methods as well as values of attributes get
serialized to BSON with L<Mango::BSON>, which provides many helper functions
you can use to generate data types that are not available natively in Perl.
All connections will be reset automatically if a new process has been forked,
this allows multiple processes to share the same L<Mango> object safely.

For better scalability (epoll, kqueue) and to provide IPv6, SOCKS5 as well as
TLS support, the optional modules L<EV> (4.0+), L<IO::Socket::IP> (0.20+),
L<IO::Socket::Socks> (0.64+) and L<IO::Socket::SSL> (1.84+) will be used
automatically if they are installed. Individual features can also be disabled
with the C<MOJO_NO_IPV6>, C<MOJO_NO_SOCKS> and C<MOJO_NO_TLS> environment
variables.

=head1 EVENTS

L<Mango> inherits all events from L<Mojo::EventEmitter> and can emit the
following new ones.

=head2 connection

  $mango->on(connection => sub {
    my ($mango, $id) = @_;
    ...
  });

Emitted when a new connection has been established.

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
  $mango    = $mango->hosts([['localhost', 3000], ['localhost', 4000]]);

Servers to connect to, defaults to C<localhost> and port C<27017>.

=head2 inactivity_timeout

  my $timeout = $mango->inactivity_timeout;
  $mango      = $mango->inactivity_timeout(15);

Maximum amount of time in seconds a connection can be inactive before getting
closed, defaults to C<0>. Setting the value to C<0> will allow connections to
be inactive indefinitely.

=head2 ioloop

  my $loop = $mango->ioloop;
  $mango   = $mango->ioloop(Mojo::IOLoop->new);

Event loop object to use for blocking I/O operations, defaults to a
L<Mojo::IOLoop> object.

=head2 j

  my $j  = $mango->j;
  $mango = $mango->j(1);

Wait for all operations to have reached the journal, defaults to C<0>.

=head2 max_bson_size

  my $max = $mango->max_bson_size;
  $mango  = $mango->max_bson_size(16777216);

Maximum size for BSON documents in bytes, defaults to C<16777216> (16MB).

=head2 max_connections

  my $max = $mango->max_connections;
  $mango  = $mango->max_connections(5);

Maximum number of connections to use for non-blocking operations, defaults to
C<5>.

=head2 max_write_batch_size

  my $max = $mango->max_write_batch_size;
  $mango  = $mango->max_write_batch_size(1000);

 Maximum number of write operations to batch together, defaults to C<1000>.

=head2 protocol

  my $protocol = $mango->protocol;
  $mango       = $mango->protocol(Mango::Protocol->new);

Protocol handler, defaults to a L<Mango::Protocol> object.

=head2 w

  my $w  = $mango->w;
  $mango = $mango->w(2);

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

=head2 backlog

  my $num = $mango->backlog;

Number of queued operations that have not yet been assigned to a connection.

=head2 db

  my $db = $mango->db;
  my $db = $mango->db('test');

Build L<Mango::Database> object for database, uses L</"default_db"> if no name
is provided. Note that the reference L<Mango::Database/"mango"> is weakened,
so the L<Mango> object needs to be referenced elsewhere as well.

=head2 from_string

  $mango
    = $mango->from_string('mongodb://sri:s3cret@localhost:3000/test?w=2');

Parse configuration from connection string.

=head2 get_more

  my $reply = $mango->get_more($namespace, $return, $cursor);

Perform low level C<GET_MORE> operation. You can also append a callback to
perform operation non-blocking.

  $mango->get_more(($namespace, $return, $cursor) => sub {
    my ($mango, $err, $reply) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 kill_cursors

  $mango->kill_cursors(@ids);

Perform low level C<KILL_CURSORS> operation. You can also append a callback to
perform operation non-blocking.

    $mango->kill_cursors(@ids => sub {
      my ($mango, $err) = @_;
      ...
    });
    Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 new

  my $mango = Mango->new;
  my $mango = Mango->new('mongodb://sri:s3cret@localhost:3000/test?w=2');

Construct a new L<Mango> object and parse connection string with
L</"from_string"> if necessary.

=head2 query

  my $reply
    = $mango->query($namespace, $flags, $skip, $return, $query, $fields);

Perform low level C<QUERY> operation. You can also append a callback to
perform operation non-blocking.

  $mango->query(($namespace, $flags, $skip, $return, $query, $fields) => sub {
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

Colin Cyr

=back

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2013-2014, Sebastian Riedel.

This program is free software, you can redistribute it and/or modify it under
the terms of the Artistic License version 2.0.

=head1 SEE ALSO

L<https://github.com/kraih/mango>, L<Mojolicious::Guides>,
L<http://mojolicio.us>.

=cut
