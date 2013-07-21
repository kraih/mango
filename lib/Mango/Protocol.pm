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
  my $vec = pack 'B*', '0' x 32;
  vec($vec, 0, 1) = 1 if $flags->{single_remove};
  $msg .= encode_int32(unpack 'V', $vec);

  # Query
  $msg .= bson_encode $query;

  # Header
  return _build_header($id, length($msg), DELETE) . $msg;
}

sub build_get_more {
  my ($self, $id, $name, $return, $cursor) = @_;

  # Zero and name
  my $msg = encode_int32(0) . encode_cstring($name);

  # Number to return and cursor id
  $msg .= encode_int32($return) . encode_int64($cursor);

  # Header
  return _build_header($id, length($msg), GET_MORE) . $msg;
}

sub build_insert {
  my ($self, $id, $name, $flags) = (shift, shift, shift, shift);

  # Flags
  my $vec = pack 'B*', '0' x 32;
  vec($vec, 0, 1) = 1 if $flags->{continue_on_error};
  my $msg = encode_int32(unpack 'V', $vec);

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
  my ($self, $id, $name, $flags, $skip, $return, $query, $fields) = @_;

  # Flags
  my $vec = pack 'B*', '0' x 32;
  vec($vec, 1, 1) = 1 if $flags->{tailable_cursor};
  vec($vec, 2, 1) = 1 if $flags->{slave_ok};
  vec($vec, 4, 1) = 1 if $flags->{no_cursor_timeout};
  vec($vec, 5, 1) = 1 if $flags->{await_data};
  vec($vec, 6, 1) = 1 if $flags->{exhaust};
  vec($vec, 7, 1) = 1 if $flags->{partial};
  my $msg = encode_int32(unpack 'V', $vec);

  # Name
  $msg .= encode_cstring $name;

  # Skip and number to return
  $msg .= encode_int32($skip) . encode_int32($return);

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
  my $vec = pack 'B*', '0' x 32;
  vec($vec, 0, 1) = 1 if $flags->{upsert};
  vec($vec, 1, 1) = 1 if $flags->{multi_update};
  $msg .= encode_int32(unpack 'V', $vec);

  # Query and update specification
  $msg .= bson_encode($query) . bson_encode($update);

  # Header
  return _build_header($id, length($msg), UPDATE) . $msg;
}

sub command_error {
  my ($self, $reply) = @_;
  my $doc = $reply->{docs}[0];
  return $doc->{ok} ? $doc->{err} : $doc->{errmsg};
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
  my $id = decode_int32(substr $msg, 0, 4, '');
  my $to = decode_int32(substr $msg, 0, 4, '');
  my $op = decode_int32(substr $msg, 0, 4, '');
  return undef unless $op == REPLY;

  # Flags
  my $flags = {};
  my $vec = substr $msg, 0, 4, '';
  $flags->{cursor_not_found} = vec $vec, 0, 1;
  $flags->{query_failure}    = vec $vec, 1, 1;
  $flags->{await_capable}    = vec $vec, 3, 1;

  # Cursor id
  my $cursor = decode_int64(substr $msg, 0, 8, '');

  # Starting from
  my $from = decode_int32(substr $msg, 0, 4, '');

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

sub _build_header {
  my ($id, $length, $op) = @_;
  return join '', map { encode_int32($_) } $length + 16, $id, 0, $op;
}

1;

=encoding utf8

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

Build message for C<delete> operation.

=head2 build_get_more

  my $bytes = $protocol->build_get_more($id, $name, $return, $cursor);

Build message for C<get_more> operation.

=head2 build_insert

  my $bytes = $protocol->build_insert($id, $name, $flags, @docs);

Build message for C<insert> operation.

=head2 build_kill_cursors

  my $bytes = $protocol->build_kill_cursors($id, @ids);

Build message for C<kill_cursors> operation.

=head2 build_query

  my $bytes = $protocol->build_query($id, $name, $flags, $skip, $return,
    $query, $fields);

Build message for C<query> operation.

=head2 build_update

  my $bytes = $protocol->build_update($id, $name, $flags, $query, $update);

Build message for C<update> operation.

=head2 command_error

  my $err = $protocol->command_error($reply);

Check reply for command error.

=head2 next_id

  my $id = $protocol->next_id(23);

Generate next id.

=head2 parse_reply

  my $reply = $protocol->parse_reply(\$str);

Extract and parse C<reply> message.

=head2 query_failure

  my $err = $protocol->query_failure($reply);

Check reply for query failure.

=head1 SEE ALSO

L<Mango>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
