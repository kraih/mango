package Mango::Protocol;
use Mojo::Base -base;

use Mango::BSON qw(bson_decode bson_encode bson_length decode_int32),
  qw(decode_int64 encode_cstring encode_int32 encode_int64);

# Opcodes
use constant {
  REPLY        => 1,
  UPDATE       => 2001,
  INSERT       => 2002,
  QUERY        => 2004,
  GET_MORE     => 2005,
  DELETE       => 2006,
  KILL_CURSORS => 2007
};

sub build_delete {
  my ($self, $id, $name, $flags, $query) = @_;

  # Zero and name
  my $msg = encode_int32(0) . encode_cstring($name);

  # Flags
  my $vec = 0b00000000000000000000000000000000;
  _set($vec, 0b10000000000000000000000000000000, $flags->{single_remove});
  $msg .= encode_int32 $vec;

  # Query
  $msg .= bson_encode $query;

  # Header
  return _build_header($id, length($msg), DELETE) . $msg;
}

sub build_get_more {
  my ($self, $id, $name, $limit, $cursor) = @_;

  # Zero and name
  my $msg = encode_int32(0) . encode_cstring($name);

  # Limit and cursor id
  $msg .= encode_int32($limit) . encode_int64($cursor);

  # Header
  return _build_header($id, length($msg), GET_MORE) . $msg;
}

sub build_insert {
  my ($self, $id, $name, $flags) = (shift, shift, shift, shift);

  # Flags
  my $vec = 0b00000000000000000000000000000000;
  _set($vec, 0b10000000000000000000000000000000, $flags->{continue_on_error});
  my $msg = encode_int32 $vec;

  # Name
  $msg .= encode_cstring $name;

  # Documents
  $msg .= bson_encode $_ for @_;

  # Header
  return _build_header($id, length($msg), INSERT) . $msg;
}

sub build_kill_cursors {
  my ($self, $id) = (shift, shift);

  # Zero and number of cursor ids
  my $msg = encode_int32(0) . encode_int32(scalar @_);

  # Cursor ids
  $msg .= encode_int64 $_ for @_;

  # Header
  return _build_header($id, length($msg), KILL_CURSORS) . $msg;
}

sub build_query {
  my ($self, $id, $name, $flags, $skip, $limit, $query, $fields) = @_;

  # Flags
  my $vec = 0b00000000000000000000000000000000;
  _set($vec, 0b01000000000000000000000000000000, $flags->{tailable_cursor});
  _set($vec, 0b00100000000000000000000000000000, $flags->{slave_ok});
  _set($vec, 0b00001000000000000000000000000000, $flags->{no_cursor_timeout});
  _set($vec, 0b00000100000000000000000000000000, $flags->{await_data});
  _set($vec, 0b00000010000000000000000000000000, $flags->{exhaust});
  _set($vec, 0b00000001000000000000000000000000, $flags->{partial});
  my $msg = encode_int32 $vec;

  # Name
  $msg .= encode_cstring $name;

  # Skip and limit
  $msg .= encode_int32($skip) . encode_int32($limit);

  # Query
  $msg .= bson_encode $query;

  # Optional field selector
  $msg .= bson_encode $fields if $fields;

  # Header
  return _build_header($id, length($msg), QUERY) . $msg;
}

sub build_update {
  my ($self, $id, $name, $flags, $query, $update) = @_;

  # Zero and name
  my $msg = encode_int32(0) . encode_cstring($name);

  # Flags
  my $vec = 0b00000000000000000000000000000000;
  _set($vec, 0b10000000000000000000000000000000, $flags->{upsert});
  _set($vec, 0b01000000000000000000000000000000, $flags->{multi_update});
  $msg .= encode_int32 $vec;

  # Query and update sepecification
  $msg .= bson_encode($query) . bson_encode($update);

  # Header
  return _build_header($id, length($msg), UPDATE) . $msg;
}

sub parse_reply {
  my ($self, $bufref) = @_;

  # Make sure we have the whole message
  return undef unless my $len = bson_length $$bufref;
  return undef if length $$bufref < $len;
  my $msg = substr $$bufref, 0, $len, '';
  substr $msg, 0, 4, '';

  # Header
  my $id = decode_int32(substr $msg, 0, 4, '');
  my $to = decode_int32(substr $msg, 0, 4, '');
  my $op = decode_int32(substr $msg, 0, 4, '');
  return undef unless $op == REPLY;

  # FLags
  my $flags = {};
  my $vec = decode_int32(substr $msg, 0, 4, '');
  $flags->{cursor_not_found} = _get($vec, 0b10000000000000000000000000000000);
  $flags->{query_failure}    = _get($vec, 0b01000000000000000000000000000000);
  $flags->{await_capable}    = _get($vec, 0b00010000000000000000000000000000);

  # Cursor id
  my $cursor = decode_int64(substr $msg, 0, 8, '');

  # Starting from
  my $from = decode_int32(substr $msg, 0, 4, '');

  # Documents (remove number of documents prefix)
  substr $msg, 0, 4, '';
  my @docs;
  push @docs, bson_decode(substr $msg, 0, bson_length($msg), '') while $msg;

  return [$id, $to, $flags, $cursor, $from, \@docs];
}

sub _build_header {
  my ($id, $length, $op) = @_;
  return join '', map { encode_int32($_) } $length + 16, $id, 0, $op;
}

sub _get { (vec($_[0], 0, 32) & $_[1]) == $_[1] ? 1 : 0 }

sub _set { vec($_[0], 0, 32) |= $_[1] if $_[2] }

1;

=head1 NAME

Mango::Protocol - The MongoDB wire protocol

=head1 SYNOPSIS

  use Mango::Protocol;

  my $protocol = Mango::Protocol->new;
  my $bytes    = $protocol->insert(23, 'foo.bar', {}, {foo => 'bar'});

=head1 DESCRIPTION

L<Mango::Protocol> is a minimalistic implementation of the MongoDB wire
protocol.

=head1 METHODS

L<Mango::Protocol> inherits all methods from L<Mojo::Base> and implements the
following new ones.

=head2 build_delete

  my $bytes = $protocol->build_delete($id, $name, $flags, $query);

Build packet for C<delete> operation.

=head2 build_get_more

  my $bytes = $protocol->build_get_more($id, $name, $limit, $cursor);

Build packet for C<get_more> operation.

=head2 build_insert

  my $bytes = $protocol->build_insert($id, $name, $flags, @docs);

Build packet for C<insert> operation.

=head2 build_kill_cursors

  my $bytes = $protocol->build_kill_cursors($id, @ids);

Build packet for C<kill_cursors> operation.

=head2 build_query

  my $bytes = $protocol->build_query($id, $name, $flags, $skip, $limit,
    $query, $fields);

Build packet for C<query> operation.

=head2 build_update

  my $bytes = $protocol->build_update($id, $name, $flags, $query, $update);

Build packet for C<update> operation.

=head2 parse_reply

  my $reply = $protocol->parse_reply(\$string);

Extract and parse C<reply> packet.

=head1 SEE ALSO

L<Mango>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
