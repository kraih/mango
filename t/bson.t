package BSONTest;
use Mojo::Base -base;

has 'something' => sub { {} };

sub TO_JSON { shift->something }

package main;
use Mojo::Base -strict;

use Test::More;
use Mango::BSON ':bson';
use Mojo::ByteStream 'b';

# Sorted document
my $doc = bson_doc(a => 1, c => 2, b => 3);
$doc->{d} = 4;
$doc->{e} = 5;
is_deeply [keys %$doc],   [qw(a c b d e)], 'sorted keys';
is_deeply [values %$doc], [qw(1 2 3 4 5)], 'sorted values';
delete $doc->{c};
is_deeply [keys %$doc],   [qw(a b d e)], 'sorted keys';
is_deeply [values %$doc], [qw(1 3 4 5)], 'sorted values';
$doc->{d} = 6;
is_deeply [keys %$doc],   [qw(a b d e)], 'sorted keys';
is_deeply [values %$doc], [qw(1 3 6 5)], 'sorted values';

# Document length prefix
is bson_length("\x05"),                     undef, 'no length';
is bson_length("\x05\x00\x00\x00"),         5,     'right length';
is bson_length("\x05\x00\x00\x00\x00"),     5,     'right length';
is bson_length("\x05\x00\x00\x00\x00\x00"), 5,     'right length';

# Generate Object ID
is length bson_oid, 24, 'right length';
is bson_oid('510d83915867b405b9000000')->to_epoch, 1359840145,
  'right epoch time';

# Generate Time
is length bson_time, length(time) + 3, 'right length';
is length bson_time->to_epoch, length time, 'right length';
is substr(bson_time, 0, 5), substr(time, 0, 5), 'same start';

# Empty document
my $bson = bson_encode {};
is_deeply bson_decode($bson), {}, 'successful roundtrip';

# Minimal document roundtrip
my $bytes = "\x05\x00\x00\x00\x00";
$doc = bson_decode($bytes);
is_deeply [keys %$doc], [], 'empty document';
is_deeply $doc, {}, 'empty document';
is bson_encode($doc), $bytes, 'successful roundtrip';

# Incomplete document
is bson_decode("\x05\x00\x00\x00"), undef, 'no result';
is bson_decode("\x05\x00\x00"),     undef, 'no result';
is bson_decode("\x05\x00"),         undef, 'no result';
is bson_decode("\x05"),             undef, 'no result';

# Nested document roundtrip
$bytes = "\x10\x00\x00\x00\x03\x6e\x6f\x6e\x65\x00\x05\x00\x00\x00\x00\x00";
$doc   = bson_decode($bytes);
is_deeply $doc, {none => {}}, 'empty nested document';
is bson_encode($doc), $bytes, 'successful roundtrip for hash';
is bson_encode(bson_doc(none => {})), $bytes,
  'successful roundtrip for document';

# Document roundtrip with "0" in key
is_deeply bson_decode(bson_encode {n0ne => 'n0ne'}), bson_doc(n0ne => 'n0ne'),
  'successful roundtrip';

# String roundtrip
$bytes = "\x1b\x00\x00\x00\x02\x74\x65\x73\x74\x00\x0c\x00\x00\x00\x68\x65"
  . "\x6c\x6c\x6f\x20\x77\x6f\x72\x6c\x64\x00\x00";
$doc = bson_decode($bytes);
is $doc->{test}, 'hello world', 'right value';
is_deeply [keys %$doc], ['test'], 'one element';
is_deeply $doc, {test => 'hello world'}, 'right document';
is bson_encode($doc), $bytes, 'successful roundtrip';
$doc = bson_decode(bson_encode {foo => 'i ♥ mojolicious'});
is $doc->{foo}, 'i ♥ mojolicious', 'successful roundtrip';

# Array
$bytes
  = "\x11\x00\x00\x00\x04\x65\x6d\x70\x74\x79\x00\x05\x00\x00\x00\x00\x00";
$doc = bson_decode($bytes);
is_deeply $doc, {empty => []}, 'empty array';

# Array roundtrip
$bytes
  = "\x11\x00\x00\x00\x04\x65\x6d\x70\x74\x79\x00\x05\x00\x00\x00\x00\x00";
$doc = bson_decode($bytes);
is_deeply $doc, {empty => []}, 'empty array';
is bson_encode($doc), $bytes, 'successful roundtrip';
$bytes
  = "\x33\x00\x00\x00\x04\x66\x69\x76\x65\x00\x28\x00\x00\x00\x10\x30\x00\x01"
  . "\x00\x00\x00\x10\x31\x00\x02\x00\x00\x00\x10\x32\x00\x03\x00\x00\x00\x10"
  . "\x33\x00\x04\x00\x00\x00\x10\x34\x00\x05\x00\x00\x00\x00\x00";
$doc = bson_decode($bytes);
is_deeply $doc, {five => [1, 2, 3, 4, 5]}, 'array with five elements';
is bson_encode($doc), $bytes, 'successful roundtrip';

# Timestamp roundtrip
$bytes = "\x13\x00\x00\x00\x11\x74\x65\x73\x74\x00\x14\x00\x00\x00\x04\x00\x00"
  . "\x00\x00";
$doc = bson_decode($bytes);
isa_ok $doc->{test}, 'Mango::BSON::Timestamp', 'right reference';
is $doc->{test}->seconds,   4,  'right seconds';
is $doc->{test}->increment, 20, 'right increment';
is bson_encode($doc), $bytes, 'successful roundtrip';

# Double roundtrip
$bytes = "\x14\x00\x00\x00\x01\x68\x65\x6c\x6c\x6f\x00\x00\x00\x00\x00\x00\x00"
  . "\xf8\x3f\x00";
$doc = bson_decode($bytes);
is_deeply $doc, {hello => 1.5}, 'right document';
is bson_encode($doc), $bytes, 'successful roundtrip';
$doc = bson_decode(bson_encode {test => -1.5});
is $doc->{test}, -1.5, 'successful roundtrip';

# Int32 roundtrip
$bytes = "\x0f\x00\x00\x00\x10\x6d\x69\x6b\x65\x00\x64\x00\x00\x00\x00";
$doc   = bson_decode($bytes);
is_deeply $doc, {mike => 100}, 'right document';
is bson_encode($doc), $bytes, 'successful roundtrip';
$doc = bson_decode(bson_encode {test => -100});
is $doc->{test}, -100, 'successful roundtrip';

# Int64 roundtrip
$bytes = "\x13\x00\x00\x00\x12\x6d\x69\x6b\x65\x00\x01\x00\x00\x80\x00\x00\x00"
  . "\x00\x00";
$doc = bson_decode($bytes);
is_deeply $doc, {mike => 2147483649}, 'right document';
is bson_encode($doc), $bytes, 'successful roundtrip';
$doc = bson_decode(bson_encode {test => -2147483648});
is $doc->{test}, -2147483648, 'successful roundtrip';

# Boolean roundtrip
$bytes = "\x0c\x00\x00\x00\x08\x74\x72\x75\x65\x00\x01\x00";
$doc   = bson_decode($bytes);
is_deeply $doc, {true => bson_true()}, 'right document';
is bson_encode($doc), $bytes, 'successful roundtrip';
$bytes = "\x0d\x00\x00\x00\x08\x66\x61\x6c\x73\x65\x00\x00\x00";
$doc   = bson_decode($bytes);
is_deeply $doc, {false => bson_false()}, 'right document';
is bson_encode($doc), $bytes, 'successful roundtrip';

# Null roundtrip
$bytes = "\x0b\x00\x00\x00\x0a\x74\x65\x73\x74\x00\x00";
$doc   = bson_decode($bytes);
is_deeply $doc, {test => undef}, 'right document';
is bson_encode($doc), $bytes, 'successful roundtrip';

# Max key roundtrip
$bytes = "\x0b\x00\x00\x00\x7f\x74\x65\x73\x74\x00\x00";
$doc   = bson_decode($bytes);
is_deeply $doc, {test => bson_max()}, 'right document';
is bson_encode($doc), $bytes, 'successful roundtrip';

# Min key roundtrip
$bytes = "\x0b\x00\x00\x00\xff\x74\x65\x73\x74\x00\x00";
$doc   = bson_decode($bytes);
is_deeply $doc, {test => bson_min()}, 'right document';
is bson_encode($doc), $bytes, 'successful roundtrip';

# Object ID roundtrip
my $id = '000102030405060708090a0b';
$bytes = "\x16\x00\x00\x00\x07\x6F\x69\x64\x00\x00"
  . "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x00";
$doc = bson_decode($bytes);
isa_ok $doc->{oid}, 'Mango::BSON::ObjectID', 'right reference';
is $doc->{oid}->to_epoch, 66051, 'right epoch time';
is_deeply $doc, {oid => $id}, 'right document';
is bson_encode($doc), $bytes, 'successful roundtrip';

# Regex roundtrip
$bytes
  = "\x12\x00\x00\x00\x0B\x72\x65\x67\x65\x78\x00\x61\x2A\x62\x00\x69\x00\x00";
$doc = bson_decode($bytes);
is_deeply $doc, {regex => qr/a*b/i}, 'right document';
like 'AAB',  $doc->{regex}, 'regex works';
like 'ab',   $doc->{regex}, 'regex works';
unlike 'Ax', $doc->{regex}, 'regex works';
is bson_encode($doc), $bytes, 'successful roundtrip';

# Code roundtrip
$bytes = "\x1c\x00\x00\x00\x0d\x66\x6f\x6f\x00\x0e\x00\x00\x00\x76\x61\x72\x20"
  . "\x66\x6f\x6f\x20\x3d\x20\x32\x33\x3b\x00\x00";
$doc = bson_decode($bytes);
isa_ok $doc->{foo}, 'Mango::BSON::Code', 'right reference';
is_deeply $doc, {foo => bson_code('var foo = 23;')}, 'right document';
is bson_encode($doc), $bytes, 'successful roundtrip';

# Code with scope roundtrip
$bytes
  = "\x32\x00\x00\x00\x0f\x66\x6f\x6f\x00\x24\x00\x00\x00\x0e\x00\x00\x00\x76"
  . "\x61\x72\x20\x66\x6f\x6f\x20\x3d\x20\x32\x34\x3b\x00\x12\x00\x00\x00\x02\x66"
  . "\x6f\x6f\x00\x04\x00\x00\x00\x62\x61\x72\x00\x00\x00";
$doc = bson_decode($bytes);
isa_ok $doc->{foo}, 'Mango::BSON::Code', 'right reference';
is_deeply $doc, {foo => bson_code('var foo = 24;')->scope({foo => 'bar'})},
  'right document';
is bson_encode($doc), $bytes, 'successful roundtrip';

# Time roundtrip
$bytes = "\x14\x00\x00\x00\x09\x74\x6f\x64\x61\x79\x00\x4e\x61\xbc\x00\x00\x00"
  . "\x00\x00\x00";
$doc = bson_decode($bytes);
isa_ok $doc->{today}, 'Mango::BSON::Time', 'right reference';
is_deeply $doc, {today => bson_time(12345678)}, 'right document';
is bson_encode($doc), $bytes, 'successful roundtrip';

# Generic binary roundtrip
$bytes = "\x14\x00\x00\x00\x05\x66\x6f\x6f\x00\x05\x00\x00\x00\x00\x31\x32\x33"
  . "\x34\x35\x00";
$doc = bson_decode($bytes);
isa_ok $doc->{foo}, 'Mango::BSON::Binary', 'right reference';
is $doc->{foo}->type, 'generic', 'right type';
is_deeply $doc, {foo => bson_bin('12345')}, 'right document';
is bson_encode($doc), $bytes, 'successful roundtrip';

# Function binary roundtrip
$bytes = "\x14\x00\x00\x00\x05\x66\x6f\x6f\x00\x05\x00\x00\x00\x01\x31\x32\x33"
  . "\x34\x35\x00";
$doc = bson_decode($bytes);
isa_ok $doc->{foo}, 'Mango::BSON::Binary', 'right reference';
is $doc->{foo}->type, 'function', 'right type';
is_deeply $doc, {foo => bson_bin('12345')->type('function')}, 'right document';
is bson_encode($doc), $bytes, 'successful roundtrip';

# MD5 binary roundtrip
$bytes = "\x14\x00\x00\x00\x05\x66\x6f\x6f\x00\x05\x00\x00\x00\x05\x31\x32\x33"
  . "\x34\x35\x00";
$doc = bson_decode($bytes);
isa_ok $doc->{foo}, 'Mango::BSON::Binary', 'right reference';
is $doc->{foo}->type, 'md5', 'right type';
is_deeply $doc, {foo => bson_bin('12345')->type('md5')}, 'right document';
is bson_encode($doc), $bytes, 'successful roundtrip';

# UUID binary roundtrip
$bytes = "\x14\x00\x00\x00\x05\x66\x6f\x6f\x00\x05\x00\x00\x00\x04\x31\x32\x33"
  . "\x34\x35\x00";
$doc = bson_decode($bytes);
isa_ok $doc->{foo}, 'Mango::BSON::Binary', 'right reference';
is $doc->{foo}->type, 'uuid', 'right type';
is_deeply $doc, {foo => bson_bin('12345')->type('uuid')}, 'right document';
is bson_encode($doc), $bytes, 'successful roundtrip';

# User defined binary roundtrip
$bytes = "\x14\x00\x00\x00\x05\x66\x6f\x6f\x00\x05\x00\x00\x00\x80\x31\x32\x33"
  . "\x34\x35\x00";
$doc = bson_decode($bytes);
isa_ok $doc->{foo}, 'Mango::BSON::Binary', 'right reference';
is $doc->{foo}->type, 'user_defined', 'right type';
is_deeply $doc, {foo => bson_bin('12345')->type('user_defined')},
  'right document';
is bson_encode($doc), $bytes, 'successful roundtrip';

# Blessed reference
$bytes = bson_encode {test => b('test')};
is_deeply bson_decode($bytes), {test => 'test'}, 'successful roundtrip';

# Blessed reference with TO_JSON method
$bytes = bson_encode({test => BSONTest->new});
is_deeply bson_decode($bytes), {test => {}}, 'successful roundtrip';
$bytes = bson_encode(
  {
    test => BSONTest->new(
      something => {just => 'works'},
      else      => {not  => 'working'}
    )
  }
);
is_deeply bson_decode($bytes), {test => {just => 'works'}},
  'successful roundtrip';

# Boolean shortcut
is_deeply bson_decode(bson_encode({true => \1})), {true => bson_true},
  'encode true boolean from constant reference';
is_deeply bson_decode(bson_encode({false => \0})), {false => bson_false},
  'encode false boolean from constant reference';
$bytes = 'some true value';
is_deeply bson_decode(bson_encode({true => \!!$bytes})), {true => bson_true},
  'encode true boolean from double negated reference';
is_deeply bson_decode(bson_encode({true => \$bytes})), {true => bson_true},
  'encode true boolean from reference';
$bytes = '';
is_deeply bson_decode(bson_encode({false => \!!$bytes})),
  {false => bson_false}, 'encode false boolean from double negated reference';
is_deeply bson_decode(bson_encode({false => \$bytes})), {false => bson_false},
  'encode false boolean from reference';

done_testing();
