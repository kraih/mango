use Mojo::Base -strict;

use Test::More;
use Mango::Protocol;

# Generate next id
my $protocol = Mango::Protocol->new;
is $protocol->next_id(1),          2,          'right id';
is $protocol->next_id(2147483646), 2147483647, 'right id';
is $protocol->next_id(2147483647), 1,          'right id';

# Build minimal query
is $protocol->build_query(1, 'foo', {}, 0, 10, {}, {}),
    "\x2a\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\xd4\x07\x00\x00\x00\x00"
  . "\x00\x00\x66\x6f\x6f\x00\x00\x00\x00\x00\x0a\x00\x00\x00\x05\x00\x00\x00"
  . "\x00\x05\x00\x00\x00\x00", 'minimal query';

# Build query with all flags
my $flags = {
  tailable_cursor   => 1,
  slave_ok          => 1,
  no_cursor_timeout => 1,
  await_data        => 1,
  exhaust           => 1,
  partial           => 1
};
is $protocol->build_query(1, 'foo', $flags, 0, 10, {}, {}),
    "\x2a\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\xd4\x07\x00\x00\xf6"
  . "\x00\x00\x00\x66\x6f\x6f\x00\x00\x00\x00\x00\x0a\x00\x00\x00\x05\x00"
  . "\x00\x00\x00\x05\x00\x00\x00\x00", 'query with all flags';

# Build minimal get_more
is $protocol->build_get_more(1, 'foo', 10, 1),
  "\x24\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\xd5\x07\x00\x00\x00\x00"
  . "\x00\x00\x66\x6f\x6f\x00\x0a\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00",
  'minimal get_more';

# Build minimal kill_cursors
is $protocol->build_kill_cursors(1, 1),
  "\x20\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\xd7\x07\x00\x00\x00\x00"
  . "\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00",
  'minimal kill_cursors';

# Parse full reply with leftovers
my $buffer
  = "\x51\x00\x00\x00\x69\xaa\x04\x00\x03\x00\x00\x00\x01\x00\x00\x00\x08\x00"
  . "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00"
  . "\x2d\x00\x00\x00\x02\x6e\x6f\x6e\x63\x65\x00\x11\x00\x00\x00\x33\x32\x39"
  . "\x35\x65\x35\x63\x64\x35\x65\x65\x66\x32\x35\x30\x30\x00\x01\x6f\x6b\x00"
  . "\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x51";
my $reply = $protocol->parse_reply(\$buffer);
is $buffer, "\x51", 'right leftovers';
my $nonce = {
  id     => 305769,
  to     => 3,
  flags  => {await_capable => 1},
  cursor => 0,
  from   => 0,
  docs   => [{nonce => '3295e5cd5eef2500', ok => 1}]
};
is_deeply $reply, $nonce, 'right reply';

# Parse query failure
$buffer
  = "\x59\x00\x00\x00\x3b\xd7\x04\x00\x01\x00\x00\x00\x01\x00\x00\x00\x02\x00"
  . "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00"
  . "\x35\x00\x00\x00\x02\x24\x65\x72\x72\x00\x1c\x00\x00\x00\x24\x6f\x72\x20"
  . "\x72\x65\x71\x75\x69\x72\x65\x73\x20\x6e\x6f\x6e\x65\x6d\x70\x74\x79\x20"
  . "\x61\x72\x72\x61\x79\x00\x10\x63\x6f\x64\x65\x00\xce\x33\x00\x00\x00";
$reply = $protocol->parse_reply(\$buffer);
my $query = {
  id     => 317243,
  to     => 1,
  flags  => {query_failure => 1},
  cursor => 0,
  from   => 0,
  docs   => [{'$err' => '$or requires nonempty array', code => 13262}]
};
is_deeply $reply, $query, 'right reply';

# Parse partial reply
my $before = my $after = "\x10";
is $protocol->parse_reply(\$after), undef, 'nothing';
is $before, $after, 'no changes';
$before = $after = "\x00\x01\x00\x00";
is $protocol->parse_reply(\$after), undef, 'nothing';
is $before, $after, 'no changes';

# Parse wrong message type
$buffer = $protocol->build_query(1, 'foo', {}, 0, 10, {}, {}) . "\x00";
is $protocol->parse_reply(\$buffer), undef, 'nothing';
is $buffer, "\x00", 'message has been removed';

# Extract error messages from reply
is $protocol->query_failure($query), '$or requires nonempty array',
  'right query failure';
is $protocol->query_failure(undef), undef, 'no query failure';
is $protocol->query_failure($nonce), undef, 'no query failure';

# Extract error messages from documents
my $unknown
  = {errmsg => 'no such cmd: whatever', 'bad cmd' => {whatever => 1}, ok => 0};
my $write = {
  n           => 0,
  ok          => 1,
  writeErrors => [
    {
      code   => 11000,
      errmsg => 'insertDocument :: caused by :: 11000 E11000 duplicate'
        . ' key error index: test.collection_test.$_id_  dup key: '
        . '{ : ObjectId(\'53408aad5867b46961a50000\') }',
      index => 0
    }
  ]
};
is $protocol->command_error($unknown), 'no such cmd: whatever', 'right error';
is $protocol->command_error($write), undef, 'no error';
like $protocol->write_error($write),
  qr/^Write error at index 0: insertDocument/, 'right error';
is $protocol->write_error($unknown), undef, 'no error';

done_testing();
