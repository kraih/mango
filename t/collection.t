use Mojo::Base -strict;

use Test::More;
use Mango;
use Mojo::IOLoop;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

# Collection names
my $mango      = Mango->new($ENV{TEST_ONLINE});
my $collection = $mango->db->collection('collection_test');
$collection->remove;
is $collection->name, 'collection_test', 'right collection name';
is $collection->full_name, join('.', $mango->db->name, $collection->name),
  'right full collection name';

# Insert documents blocking
my $oids = $collection->insert([{foo => 'bar'}, {foo => 'baz'}]);
isa_ok $oids->[0], 'Mango::BSON::ObjectID', 'right reference';
isa_ok $oids->[1], 'Mango::BSON::ObjectID', 'right reference';
is $collection->find_one($oids->[0])->{foo}, 'bar', 'right value';
is $collection->find_one($oids->[1])->{foo}, 'baz', 'right value';

# Update documents blocking
is $collection->update({}, {'$set' => {bar => 'works'}}, {multi => 1})->{n},
  2, 'two documents updated';
is $collection->update({}, {'$set' => {baz => 'too'}})->{n}, 1,
  'one document updated';
is $collection->find_one($oids->[0])->{bar}, 'works', 'right value';
is $collection->find_one($oids->[1])->{bar}, 'works', 'right value';
is $collection->update({missing => 1}, {now => 'there'}, {upsert => 1})->{n},
  1, 'one document updated';
is $collection->update({missing => 1}, {now => 'there'}, {upsert => 1})->{n},
  1, 'one document updated';
is $collection->remove({now => 'there'}, {single => 1})->{n}, 1,
  'one document removed';
is $collection->remove({now => 'there'}, {single => 1})->{n}, 1,
  'one document removed';

# Remove one document blocking
is $collection->remove({foo => 'baz'})->{n}, 1, 'one document removed';
ok $collection->find_one($oids->[0]), 'document still exists';
ok !$collection->find_one($oids->[1]), 'no document';
is $collection->remove->{n}, 1, 'one document removed';
ok !$collection->find_one($oids->[0]), 'no document';

done_testing();
