use Mojo::Base -strict;

use Test::More;
use Mango;
use Mojo::IOLoop;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

# Collection names
my $mango      = Mango->new($ENV{TEST_ONLINE});
my $collection = $mango->db->collection('collection_test');
$collection->drop;
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

# Drop collection blocking
my $oid = $collection->insert({just => 'works'});
is $collection->find_one($oid)->{just}, 'works', 'right document';
is $collection->drop->{ns}, $collection->full_name, 'right collection';
ok !$collection->find_one($oid), 'no document';

# Drop collection non-blocking
$oid = $collection->insert({just => 'works'});
is $collection->find_one($oid)->{just}, 'works', 'right document';
my ($fail, $ns);
$collection->drop(
  sub {
    my ($collection, $err, $doc) = @_;
    $fail = $err;
    $ns   = $doc->{ns};
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$mango->is_active, 'no operations in progress';
ok !$fail, 'no error';
is $ns, $collection->full_name, 'right collection';
ok !$collection->find_one($oid), 'no document';

# Ensure index blocking
$collection->insert({test => 23, foo => 'bar'});
$collection->insert({test => 23, foo => 'baz'});
is $collection->find({})->count, 2, 'two documents';
$collection->ensure_index({test => 1}, {unique => \1, dropDups => \1});
is $collection->find({})->count, 1, 'one document';
$collection->drop;

# Ensure index non-blocking
$collection->insert({test => 23, foo => 'bar'});
$collection->insert({test => 23, foo => 'baz'});
is $collection->find({})->count, 2, 'two documents';
$collection->ensure_index(
  ({test => 1}, {unique => \1, dropDups => \1}) => sub {
    my ($collection, $err) = @_;
    $fail = $err;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$mango->is_active, 'no operations in progress';
ok !$fail, 'no error';
is $collection->find({})->count, 1, 'one document';
$collection->drop;

# Create capped collection blocking
$collection->create({capped => \1, max => 2, size => 100000});
$collection->insert([{test => 1}, {test => 2}]);
is $collection->find({})->count, 2, 'two documents';
$collection->insert({test => 3});
is $collection->find({})->count, 2, 'two documents';
$collection->drop;

# Create capped collection non-blocking
$fail = undef;
$collection->create(
  {capped => \1, max => 2, size => 100000} => sub {
    my ($collection, $err) = @_;
    $fail = $err;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$mango->is_active, 'no operations in progress';
ok !$fail, 'no error';
$collection->insert([{test => 1}, {test => 2}]);
is $collection->find({})->count, 2, 'two documents';
$collection->insert({test => 3});
is $collection->find({})->count, 2, 'two documents';
$collection->drop;

done_testing();
