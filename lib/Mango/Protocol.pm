package Mango::Protocol;
use Mojo::Base -base;

use Mango::BSON qw(bson_decode bson_encode bson_length encode_cstring);

# Opcodes
use constant {REPLY => 1, QUERY => 2004, GET_MORE => 2005,
  KILL_CURSORS => 2007};

sub build_get_more {
  my ($self, $id, $name, $return, $cursor) = @_;

  # Zero and name
  my $msg = pack('l<', 0) . encode_cstring($name);

  # Number to return and cursor id
  $msg .= pack('l<', $return) . pack('q<', $cursor);

  # Header
  return _build_header($id, length($msg), GET_MORE) . $msg;
}

sub build_kill_cursors {
  my ($self, $id) = (shift, shift);

  # Zero and number of cursor ids
  my $msg = pack('l<', 0) . pack('l<', scalar @_);

  # Cursor ids
  $msg .= pack 'q<', $_ for @_;

  # Header
  return _build_header($id, length($msg), KILL_CURSORS) . $msg;
}

sub build_query {
  my ($self, $id, $name, $flags, $skip, $return, $query, $fields) = @_;

  # Flags
  my $vec = pack 'B*', '0' x 32;
  vec($vec, 1, 1) = 1 if $flags->{tailable_cursor};
  vec($vec, 2, 1) = 1 if $flags->{slave_ok};
  vec($vec, 4, 1) = 1 if $flags->{no_cursor_timeout};
  vec($vec, 5, 1) = 1 if $flags->{await_data};
  vec($vec, 6, 1) = 1 if $flags->{exhaust};
  vec($vec, 7, 1) = 1 if $flags->{partial};
  my $msg = pack 'l<', unpack('V', $vec);

  # Name
  $msg .= encode_cstring $name;

  # Skip and number to return
  $msg .= pack('l<', $skip) . pack('l<', $return);

  # Query
  $msg .= bson_encode $query;

  # Optional field selector
  $msg .= bson_encode $fields if $fields;

  # Header
  return _build_header($id, length($msg), QUERY) . $msg;
}

sub command_error {
  my ($self, $doc) = @_;
  return $doc->{ok} ? undef : $doc->{errmsg};
}

sub next_id { $_[1] > 2147483646 ? 1 : $_[1] + 1 }

sub parse_reply {
  my ($self, $bufref) = @_;

  # Make sure we have the whole message
  return undef unless my $len = bson_length $$bufref;
  return undef if length $$bufref < $len;
  my $msg = substr $$bufref, 0, $len, '';
  substr $msg, 0, 4, '';

  # Header
  my $id = unpack 'l<', substr($msg, 0, 4, '');
  my $to = unpack 'l<', substr($msg, 0, 4, '');
  my $op = unpack 'l<', substr($msg, 0, 4, '');
  return undef unless $op == REPLY;

  # Flags
  my $flags = {};
  my $vec = substr $msg, 0, 4, '';
  $flags->{cursor_not_found} = 1 if vec $vec, 0, 1;
  $flags->{query_failure}    = 1 if vec $vec, 1, 1;
  $flags->{await_capable}    = 1 if vec $vec, 3, 1;

  # Cursor id
  my $cursor = unpack 'q<', substr($msg, 0, 8, '');

  # Starting from
  my $from = unpack 'l<', substr($msg, 0, 4, '');

  # Documents (remove number of documents prefix)
  substr $msg, 0, 4, '';
  my @docs;
  push @docs, bson_decode(substr $msg, 0, bson_length($msg), '') while $msg;

  return {
    id     => $id,
    to     => $to,
    flags  => $flags,
    cursor => $cursor,
    from   => $from,
    docs   => \@docs
  };
}

sub query_failure {
  my ($self, $reply) = @_;
  return undef unless $reply;
  return $reply->{flags}{query_failure} ? $reply->{docs}[0]{'$err'} : undef;
}

sub write_error {
  my ($self, $doc) = @_;
  return undef unless my $errors = $doc->{writeErrors};
  return join "\n",
    map {"Write error at index $_->{index}: $_->{errmsg}"} @$errors;
}

sub _build_header {
  my ($id, $length, $op) = @_;
  return join '', map { pack 'l<', $_ } $length + 16, $id, 0, $op;
}

1;

=encoding utf8

=head1 NAME

Mango::Protocol - The MongoDB wire protocol

=head1 SYNOPSIS

  use Mango::Protocol;

  my $protocol = Mango::Protocol->new;
  my $bytes    = $protocol->query(1, 'foo', {}, 0, 10, {}, {});

=head1 DESCRIPTION

L<Mango::Protocol> is a minimalistic implementation of the MongoDB wire
protocol.

=head1 METHODS

L<Mango::Protocol> inherits all methods from L<Mojo::Base> and implements the
following new ones.

=head2 build_get_more

  my $bytes = $protocol->build_get_more($id, $name, $return, $cursor);

Build message for C<get_more> operation.

=head2 build_kill_cursors

  my $bytes = $protocol->build_kill_cursors($id, @ids);

Build message for C<kill_cursors> operation.

=head2 build_query

  my $bytes = $protocol->build_query($id, $name, $flags, $skip, $return,
    $query, $fields);

Build message for C<query> operation.

=head2 command_error

  my $err = $protocol->command_error($doc);

Check document for command error.

=head2 next_id

  my $id = $protocol->next_id(23);

Generate next id.

=head2 parse_reply

  my $reply = $protocol->parse_reply(\$str);

Extract and parse C<reply> message.

=head2 query_failure

  my $err = $protocol->query_failure($reply);

Check reply for query failure.

=head2 write_error

  my $err = $protocol->write_error($doc);

Check document for write error.

=head1 SEE ALSO

L<Mango>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
