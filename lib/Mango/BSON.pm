package Mango::BSON;
use Mojo::Base -strict;

use re 'regexp_pattern';
use B;
use Carp 'croak';
use Exporter 'import';
use Mango::BSON::Binary;
use Mango::BSON::Code;
use Mango::BSON::Document;
use Mango::BSON::ObjectID;
use Mango::BSON::Time;
use Mango::BSON::Timestamp;
use Mojo::JSON;
use Scalar::Util 'blessed';

my @BSON = (
  qw(bson_bin bson_code bson_decode bson_doc bson_encode bson_false),
  qw(bson_length bson_max bson_min bson_oid bson_time bson_true bson_ts)
);
our @EXPORT_OK = (
  @BSON,
  qw(decode_int32 decode_int64 encode_cstring encode_int32 encode_int64),
);
our %EXPORT_TAGS = (bson => \@BSON);

# Types
use constant {
  DOUBLE     => "\x01",
  STRING     => "\x02",
  DOCUMENT   => "\x03",
  ARRAY      => "\x04",
  BINARY     => "\x05",
  OBJECT_ID  => "\x07",
  BOOL       => "\x08",
  DATETIME   => "\x09",
  NULL       => "\x0a",
  REGEX      => "\x0b",
  CODE       => "\x0d",
  CODE_SCOPE => "\x0f",
  INT32      => "\x10",
  TIMESTAMP  => "\x11",
  INT64      => "\x12",
  MIN_KEY    => "\x7f",
  MAX_KEY    => "\xff"
};

# Binary subtypes
use constant {
  BINARY_GENERIC      => "\x00",
  BINARY_FUNCTION     => "\x01",
  BINARY_UUID         => "\x04",
  BINARY_MD5          => "\x05",
  BINARY_USER_DEFINED => "\x80"
};

# 32bit integer range
use constant {INT32_MIN => -(1 << 31) + 1, INT32_MAX => (1 << 31) - 1};

# Reuse boolean singletons
my $FALSE = Mojo::JSON->false;
my $TRUE  = Mojo::JSON->true;

my $MAXKEY = bless {}, 'Mango::BSON::_MaxKey';
my $MINKEY = bless {}, 'Mango::BSON::_MinKey';

sub bson_bin { Mango::BSON::Binary->new(data => shift) }

sub bson_code { Mango::BSON::Code->new(code => shift) }

sub bson_decode {
  my $bson = shift;
  return undef unless my $len = bson_length($bson);
  return length $bson == $len ? _decode_doc(\$bson) : undef;
}

sub bson_doc {
  tie my %hash, 'Mango::BSON::Document', @_;
  return \%hash;
}

sub bson_encode {
  my $doc = shift;

  my $bson = '';
  while (my ($key, $value) = each %$doc) {
    $bson .= _encode_value(encode_cstring($key), $value);
  }

  # Document ends with null byte
  return encode_int32(length($bson) + 5) . $bson . "\x00";
}

sub bson_false {$FALSE}

sub bson_length { length $_[0] < 4 ? undef : decode_int32(substr $_[0], 0, 4) }

sub bson_max {$MAXKEY}

sub bson_min {$MINKEY}

sub bson_oid { Mango::BSON::ObjectID->new(@_) }

sub bson_time { Mango::BSON::Time->new(@_) }

sub bson_ts {
  Mango::BSON::Timestamp->new(seconds => shift, increment => shift);
}

sub bson_true {$TRUE}

sub decode_int32 { unpack 'l<', shift }
sub decode_int64 { unpack 'q<', shift }

sub encode_cstring {
  my $str = shift;
  utf8::encode $str;
  return pack 'Z*', $str;
}

sub encode_int32 { pack 'l<', shift }
sub encode_int64 { pack 'q<', shift }

sub _decode_binary {
  my $bsonref = shift;

  my $len = decode_int32(substr $$bsonref, 0, 4, '');
  my $subtype = substr $$bsonref, 0, 1, '';
  my $binary = substr $$bsonref, 0, $len, '';

  return bson_bin($binary)->type('function') if $subtype eq BINARY_FUNCTION;
  return bson_bin($binary)->type('md5')      if $subtype eq BINARY_MD5;
  return bson_bin($binary)->type('uuid')     if $subtype eq BINARY_UUID;
  return bson_bin($binary)->type('user_defined')
    if $subtype eq BINARY_USER_DEFINED;
  return bson_bin($binary)->type('generic');
}

sub _decode_cstring {
  my $bsonref = shift;
  $$bsonref =~ s/^([^\x00]*)\x00//;
  my $str = $1;
  utf8::decode $str;
  return $str;
}

sub _decode_doc {
  my $bsonref = shift;

  # Every element starts with a type
  my $doc = bson_doc();
  substr $$bsonref, 0, 4, '';
  while (my $type = substr $$bsonref, 0, 1, '') {

    # Null byte (end of document)
    last if $type eq "\x00";

    my $name = _decode_cstring($bsonref);
    $doc->{$name} = _decode_value($type, $bsonref);
  }

  return $doc;
}

sub _decode_string {
  my $bsonref = shift;

  my $len = decode_int32(substr $$bsonref, 0, 4, '');
  substr $$bsonref, $len - 1, 1, '';
  my $str = substr $$bsonref, 0, $len - 1, '';
  utf8::decode $str;

  return $str;
}

sub _decode_value {
  my ($type, $bsonref) = @_;

  # String
  return _decode_string($bsonref) if $type eq STRING;

  # Object ID
  return bson_oid(unpack 'H*', substr $$bsonref, 0, 12, '')
    if $type eq OBJECT_ID;

  # Double/Int32/Int64
  return unpack 'd<', substr $$bsonref, 0, 8, '' if $type eq DOUBLE;
  return decode_int32(substr $$bsonref, 0, 4, '') if $type eq INT32;
  return decode_int64(substr $$bsonref, 0, 8, '') if $type eq INT64;

  # Document
  return _decode_doc($bsonref) if $type eq DOCUMENT;

  # Array
  return [values %{_decode_doc($bsonref)}] if $type eq ARRAY;

  # Booleans and Null
  return substr($$bsonref, 0, 1, '') eq "\x00" ? bson_false() : bson_true()
    if $type eq BOOL;
  return undef if $type eq NULL;

  # Time
  return bson_time(decode_int64(substr $$bsonref, 0, 8, ''))
    if $type eq DATETIME;

  # Regex
  return eval join '/', 'qr', _decode_cstring($bsonref),
    _decode_cstring($bsonref)
    if $type eq REGEX;

  # Binary (with subtypes)
  return _decode_binary($bsonref) if $type eq BINARY;

  # Min/Max
  return bson_min() if $type eq MIN_KEY;
  return bson_max() if $type eq MAX_KEY;

  # Code (with and without scope)
  return bson_code(_decode_string($bsonref)) if $type eq CODE;
  if ($type eq CODE_SCOPE) {
    decode_int32(substr $$bsonref, 0, 4, '');
    return bson_code(_decode_string($bsonref))->scope(_decode_doc($bsonref));
  }

  # Timestamp
  return bson_ts(
    reverse map({decode_int32(substr $$_, 0, 4, '')} $bsonref, $bsonref))
    if $type eq TIMESTAMP;

  # Unknown
  croak 'Unknown BSON type';
}

sub _encode_binary {
  my ($e, $subtype, $value) = @_;
  return BINARY . $e . encode_int32(length $value) . $subtype . $value;
}

sub _encode_object {
  my ($e, $value, $class) = @_;

  # ObjectID
  return OBJECT_ID . $e . pack('H*', $value)
    if $class eq 'Mango::BSON::ObjectID';

  # Time
  return DATETIME . $e . encode_int64($value) if $class eq 'Mango::BSON::Time';

  # Regex
  if ($class eq 'Regexp') {
    my ($p, $m) = regexp_pattern($value);
    return REGEX . $e . encode_cstring($p) . encode_cstring($m);
  }

  # Binary
  if ($class eq 'Mango::BSON::Binary') {
    my $type = $value->type // 'generic';
    my $data = $value->data;
    return _encode_binary($e, BINARY_FUNCTION, $data) if $type eq 'function';
    return _encode_binary($e, BINARY_MD5,      $data) if $type eq 'md5';
    return _encode_binary($e, BINARY_USER_DEFINED, $data)
      if $type eq 'user_defined';
    return _encode_binary($e, BINARY_UUID, $data) if $type eq 'uuid';
    return _encode_binary($e, BINARY_GENERIC, $data);
  }

  # Code
  if ($class eq 'Mango::BSON::Code') {

    # With scope
    if (my $scope = $value->scope) {
      my $code = _encode_string($value->code) . bson_encode($scope);
      return CODE_SCOPE . $e . encode_int32(length $code) . $code;
    }

    # Without scope
    return CODE . $e . _encode_string($value->code);
  }

  # Timestamp
  return join '', TIMESTAMP, $e, map { encode_int32 $_} $value->increment,
    $value->seconds
    if $class eq 'Mango::BSON::Timestamp';

  # Blessed reference with TO_JSON method
  if (my $sub = $value->can('TO_JSON')) {
    return _encode_value($e, $value->$sub);
  }

  # Stringify
  return STRING . $e . _encode_string($value);
}

sub _encode_string {
  my $str = shift;
  utf8::encode $str;
  return encode_int32(length($str) + 1) . "$str\x00";
}

sub _encode_value {
  my ($e, $value) = @_;

  # Null
  return NULL . $e unless defined $value;

  # Blessed
  if (my $class = blessed $value) {

    # True
    return BOOL . $e . "\x01" if $value eq $TRUE;

    # False
    return BOOL . $e . "\x00" if $value eq $FALSE;

    # Max
    return MAX_KEY . $e if $value eq $MAXKEY;

    # Min
    return MIN_KEY . $e if $value eq $MINKEY;

    # Multiple classes
    return _encode_object($e, $value, $class);
  }

  # Reference
  elsif (my $ref = ref $value) {

    # Hash (Document)
    return DOCUMENT . $e . bson_encode($value) if $ref eq 'HASH';

    # Array
    if ($ref eq 'ARRAY') {
      my $array = bson_doc();
      my $i     = 0;
      $array->{$i++} = $_ for @$value;
      return ARRAY . $e . bson_encode($array);
    }

    # Scalar (boolean shortcut)
    return _encode_value($e, $$value ? $TRUE : $FALSE) if $ref eq 'SCALAR';
  }

  # Double
  my $flags = B::svref_2object(\$value)->FLAGS;
  return DOUBLE . $e . pack('d<', $value) if $flags & B::SVp_NOK;

  if ($flags & B::SVp_IOK) {

    # Int32
    return INT32 . $e . encode_int32($value)
      if $value <= INT32_MAX && $value >= INT32_MIN;

    # Int64
    return INT64 . $e . encode_int64($value);
  }

  # String
  return STRING . $e . _encode_string("$value");
}

# Constants
package Mango::BSON::_MaxKey;

package Mango::BSON::_MinKey;

1;

=encoding utf8

=head1 NAME

Mango::BSON - BSON

=head1 SYNOPSIS

  use Mango::BSON ':bson';

  my $bson = bson_encode bson_doc(now => bson_time, counter => 13);
  my $doc  = bson_decode $bson;

=head1 DESCRIPTION

L<Mango::BSON> is a minimalistic implementation of L<http://bsonspec.org>.

In addition to a bunch of custom BSON data types it supports normal Perl data
types like C<Scalar>, C<Regexp>, C<undef>, C<Array> reference, C<Hash>
reference and will try to call the C<TO_JSON> method on blessed references, or
stringify them if it doesn't exist. C<Scalar> references will be used to
generate booleans, based on if their values are true or false.

=head1 FUNCTIONS

L<Mango::BSON> implements the following functions.

=head2 bson_bin

  my $bin = bson_bin $bytes;

Create new BSON element of the binary type with L<Mango::BSON::Binary>,
defaults to the C<generic> binary subtype.

  # Function
  bson_bin($bytes)->type('function');

  # MD5
  bson_bin($bytes)->type('md5');

  # UUID
  bson_bin($bytes)->type('uuid');

  # User defined
  bson_bin($bytes)->type('user_defined');

=head2 bson_code

  my $code = bson_code 'function () {}';

Create new BSON element of the code type with L<Mango::BSON::Code>.

  # With scope
  bson_code('function () {}')->scope({foo => 'bar'});

=head2 bson_decode

  my $doc = bson_decode $bson;

Decode BSON into Perl data structures.

=head2 bson_doc

  my $doc = bson_doc;
  my $doc = bson_doc foo => 'bar', baz => 23;

Create new BSON document with L<Mango::BSON::Document>, defaults to an empty
ordered hash.

=head2 bson_encode

  my $bson = bson_encode $doc;
  my $bson = bson_encode {};

Encode Perl data structures into BSON.

=head2 bson_false

  my $false = bson_false;

Create new BSON element of the boolean type false.

=head2 bson_length

  my $len = bson_length $bson;

Check BSON length prefix.

=head2 bson_max

  my $max_key = bson_max;

Create new BSON element of the max key type.

=head2 bson_min

  my $min_key = bson_min;

Create new BSON element of the min key type.

=head2 bson_oid

  my $oid = bson_oid;
  my $oid = bson_oid '1a2b3c4e5f60718293a4b5c6';

Create new BSON element of the object id type with L<Mango::BSON::ObjectID>,
defaults to generating a new unique object id.

=head2 bson_time

  my $now  = bson_time;
  my $time = bson_time time * 1000;

Create new BSON element of the UTC datetime type with L<Mango::BSON::Time>,
defaults to milliseconds since the UNIX epoch.

=head2 bson_true

  my $true = bson_true;

Create new BSON element of the boolean type true.

=head2 bson_ts

  my $timestamp = bson_ts 23, 24;

Create new BSON element of the timestamp type with L<Mango::BSON::Timestamp>.

=head2 decode_int32

  my $int32 = decode_int32 $bytes;

Decode 32bit integer.

=head2 decode_int64

  my $int64 = decode_int64 $bytes;

Decode 64bit integer.

=head2 encode_cstring

  my $bytes = encode_cstring $cstring;

Encode cstring.

=head2 encode_int32

  my $bytes = encode_int32 $int32;

Encode 32bit integer.

=head2 encode_int64

  my $bytes = encode_int64 $int64;

Encode 64bit integer.

=head1 SEE ALSO

L<Mango>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
