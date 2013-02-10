use Mojo::Base -strict;

use Test::More;
use List::Util 'first';
use Mango;
use Mango::BSON qw(bson_doc bson_oid);
use Mojo::IOLoop;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

# Cleanup before start
my $mango      = Mango->new($ENV{TEST_ONLINE});
my $collection = $mango->db->collection('collection_test');
$collection->drop
  if first { $_ eq 'collection_test' } @{$mango->db->collection_names};

# Collection names
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
is $collection->update({}, {'$set' => {bar => 'works'}}, {multi => 1}), 2,
  'two documents updated';
is $collection->update({}, {'$set' => {baz => 'too'}}), 1,
  'one document updated';
is $collection->find_one($oids->[0])->{bar}, 'works', 'right value';
is $collection->find_one($oids->[1])->{bar}, 'works', 'right value';
is $collection->update({missing => 1}, {now => 'there'}, {upsert => 1}), 1,
  'one document updated';
is $collection->update({missing => 1}, {now => 'there'}, {upsert => 1}), 1,
  'one document updated';
is $collection->remove({now => 'there'}, {single => 1}), 1,
  'one document removed';
is $collection->remove({now => 'there'}, {single => 1}), 1,
  'one document removed';

# Remove one document blocking
is $collection->remove({foo => 'baz'}), 1, 'one document removed';
ok $collection->find_one($oids->[0]), 'document still exists';
ok !$collection->find_one($oids->[1]), 'no document';
is $collection->remove, 1, 'one document removed';
ok !$collection->find_one($oids->[0]), 'no document';

# Find and modify document blocking
my $oid = $collection->insert({atomic => 1});
is $collection->find_one($oid)->{atomic}, 1, 'right document';
my $doc = $collection->find_and_modify(
  {query => {atomic => 1}, update => {'$set' => {atomic => 2}}});
is $doc->{atomic}, 1, 'right document';
is $collection->find_one($oid)->{atomic}, 2, 'right document';
is $collection->remove({atomic => 2}), 1, 'removed one document';

# Find and modify document non-blocking
$oid = $collection->insert({atomic => 1});
is $collection->find_one($oid)->{atomic}, 1, 'right document';
my ($fail, $result) = @_;
$collection->find_and_modify(
  {query => {atomic => 1}, update => {'$set' => {atomic => 2}}} => sub {
    my ($collection, $err, $doc) = @_;
    $fail   = $err;
    $result = $doc;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$mango->is_active, 'no operations in progress';
ok !$fail, 'no error';
is $result->{atomic}, 1, 'right document';
is $collection->find_one($oid)->{atomic}, 2, 'right document';
is $collection->remove({atomic => 2}), 1, 'removed one document';

# Aggregate collection blocking
$collection->insert([{more => 1}, {more => 2}, {more => 3}]);
my $docs = $collection->aggregate(
  [{'$group' => {_id => undef, total => {'$sum' => '$more'}}}]);
is $docs->[0]{total}, 6, 'right result';
is $collection->remove({more => {'$exists' => 1}}), 3,
  'three documents removed';

# Aggregate collection non-blocking
$collection->insert([{more => 1}, {more => 2}, {more => 3}]);
$fail = $result = undef;
$collection->aggregate(
  [{'$group' => {_id => undef, total => {'$sum' => '$more'}}}] => sub {
    my ($collection, $err, $docs) = @_;
    $fail   = $err;
    $result = $docs;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$mango->is_active, 'no operations in progress';
ok !$fail, 'no error';
is $result->[0]{total}, 6, 'right result';
is $collection->remove({more => {'$exists' => 1}}), 3,
  'three documents removed';

# Save document blocking
$oid = $collection->save({update => 'me'});
$doc = $collection->find_one($oid);
is $doc->{update}, 'me', 'right document';
$doc->{update} = 'too';
is $collection->save($doc), $oid, 'same object id';
$doc = $collection->find_one($oid);
is $doc->{update}, 'too', 'right document';
is $collection->remove({_id => $oid}), 1, 'one document removed';
$oid = bson_oid;
$doc = bson_doc _id => $oid, save => 'me';
is $collection->save($doc), $oid, 'same object id';
$doc = $collection->find_one($oid);
is $doc->{save}, 'me', 'right document';
is $collection->remove({_id => $oid}), 1, 'one document removed';

# Save document non-blocking
$fail = $result = undef;
$collection->save(
  {update => 'me'} => sub {
    my ($collection, $err, $oid) = @_;
    $fail   = $err;
    $result = $oid;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$mango->is_active, 'no operations in progress';
ok !$fail, 'no error';
$doc = $collection->find_one($result);
is $doc->{update}, 'me', 'right document';
$doc->{update} = 'too';
$oid = $result;
$fail = $result = undef;
$collection->save(
  $doc => sub {
    my ($collection, $err, $oid) = @_;
    $fail   = $err;
    $result = $oid;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$mango->is_active, 'no operations in progress';
ok !$fail, 'no error';
is $oid, $result, 'same object id';
$doc = $collection->find_one($oid);
is $doc->{update}, 'too', 'right document';
is $collection->remove({_id => $oid}), 1, 'one document removed';
$oid  = bson_oid;
$doc  = bson_doc _id => $oid, save => 'me';
$fail = $result = undef;
$collection->save(
  $doc => sub {
    my ($collection, $err, $oid) = @_;
    $fail   = $err;
    $result = $oid;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$mango->is_active, 'no operations in progress';
ok !$fail, 'no error';
is $oid, $result, 'same object id';
$doc = $collection->find_one($oid);
is $doc->{save}, 'me', 'right document';
is $collection->remove({_id => $oid}), 1, 'one document removed';

# Drop collection blocking
$oid = $collection->insert({just => 'works'});
is $collection->find_one($oid)->{just}, 'works', 'right document';
is $collection->drop->{ns}, $collection->full_name, 'right collection';
ok !$collection->find_one($oid), 'no document';

# Drop collection non-blocking
$oid = $collection->insert({just => 'works'});
is $collection->find_one($oid)->{just}, 'works', 'right document';
$fail = $result = undef;
$collection->drop(
  sub {
    my ($collection, $err, $doc) = @_;
    $fail   = $err;
    $result = $doc->{ns};
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$mango->is_active, 'no operations in progress';
ok !$fail, 'no error';
is $result, $collection->full_name, 'right collection';
ok !$collection->find_one($oid), 'no document';

# Index names
is $collection->build_index_name({foo => 1}), 'foo', 'right index name';
is $collection->build_index_name(bson_doc(foo => 1, bar => -1)), 'foo_bar',
  'right index name';
is $collection->build_index_name(bson_doc(foo => 1, 'bar.baz' => -1)),
  'foo_bar.baz', 'right index name';
is $collection->build_index_name(bson_doc(foo => 1, bar => -1, baz => '2d')),
  'foo_bar_baz', 'right index name';

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
