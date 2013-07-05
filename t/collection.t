use Mojo::Base -strict;

use Test::More;
use List::Util 'first';
use Mango;
use Mango::BSON qw(bson_code bson_doc bson_oid bson_true);
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

# Index names
is $collection->build_index_name({foo => 1}), 'foo', 'right index name';
is $collection->build_index_name(bson_doc(foo => 1, bar => -1)), 'foo_bar',
  'right index name';
is $collection->build_index_name(bson_doc(foo => 1, 'bar.baz' => -1)),
  'foo_bar.baz', 'right index name';
is $collection->build_index_name(bson_doc(foo => 1, bar => -1, baz => '2d')),
  'foo_bar_baz', 'right index name';

# Insert documents blocking
my $oids = $collection->insert([{foo => 'bar'}, {foo => 'baz'}]);
isa_ok $oids->[0], 'Mango::BSON::ObjectID', 'right class';
isa_ok $oids->[1], 'Mango::BSON::ObjectID', 'right class';
is $collection->find_one($oids->[0])->{foo}, 'bar', 'right value';
is $collection->find_one($oids->[1])->{foo}, 'baz', 'right value';

# Get collection statistics blocking
is $collection->stats->{count}, 2, 'right number of documents';

# Get collection statistics non-blocking
my ($fail, $result) = @_;
$collection->stats(
  sub {
    my ($collection, $err, $stats) = @_;
    $fail   = $err;
    $result = $stats;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$mango->is_active, 'no operations in progress';
ok !$fail, 'no error';
is $result->{count}, 2, 'right number of documents';

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
($fail, $result) = ();
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
($fail, $result) = ();
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
($fail, $result) = ();
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
($fail, $result) = ();
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
$oid = bson_oid;
$doc = bson_doc _id => $oid, save => 'me';
($fail, $result) = ();
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
$collection->drop;
ok !$collection->find_one($oid), 'no document';

# Drop collection non-blocking
$oid = $collection->insert({just => 'works'});
is $collection->find_one($oid)->{just}, 'works', 'right document';
$fail = undef;
$collection->drop(
  sub {
    my ($collection, $err) = @_;
    $fail = $err;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$mango->is_active, 'no operations in progress';
ok !$fail, 'no error';
ok !$collection->find_one($oid), 'no document';

# Ensure and drop index blocking
$collection->insert({test => 23, foo => 'bar'});
$collection->insert({test => 23, foo => 'baz'});
is $collection->find->count, 2, 'two documents';
$collection->ensure_index({test => 1}, {unique => \1, dropDups => \1});
is $collection->find->count, 1, 'one document';
is $collection->index_information->{test}{unique}, bson_true,
  'index is unique';
$collection->drop_index('test');
is $collection->index_information->{test}, undef, 'no index';
$collection->drop;

# Ensure and drop index non-blocking
$collection->insert({test => 23, foo => 'bar'});
$collection->insert({test => 23, foo => 'baz'});
is $collection->find->count, 2, 'two documents';
($fail, $result) = ();
my $delay = Mojo::IOLoop->delay(
  sub {
    my $delay = shift;
    $collection->ensure_index(
      ({test => 1}, {unique => \1, dropDups => \1}) => $delay->begin);
  },
  sub {
    my ($delay, $err) = @_;
    $fail = $err;
    $collection->index_information($delay->begin);
  },
  sub {
    my ($delay, $err, $info) = @_;
    $fail ||= $err;
    $result = $info;
  }
);
$delay->wait;
ok !$mango->is_active, 'no operations in progress';
ok !$fail, 'no error';
is $collection->find->count, 1, 'one document';
is $result->{test}{unique}, bson_true, 'index is unique';
($fail, $result) = ();
$delay = Mojo::IOLoop->delay(
  sub {
    my $delay = shift;
    $collection->drop_index(test => $delay->begin);
  },
  sub {
    my ($delay, $err) = @_;
    $fail = $err;
    $collection->index_information($delay->begin);
  },
  sub {
    my ($delay, $err, $info) = @_;
    $fail ||= $err;
    $result = $info;
  }
);
$delay->wait;
ok !$mango->is_active, 'no operations in progress';
ok !$fail, 'no error';
is $result->{test}, undef, 'no index';
$collection->drop;

# Create capped collection blocking
$collection->create({capped => \1, max => 2, size => 100000});
$collection->insert([{test => 1}, {test => 2}]);
is $collection->find({})->count, 2, 'two documents';
$collection->insert({test => 3});
is $collection->find->count, 2, 'two documents';
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
is $collection->find->count, 2, 'two documents';
$collection->drop;

# Perform map/reduce blocking
my $map = <<EOF;
function () {
  this.tags.forEach(function(z) {
    emit(z, 1);
  });
}
EOF
my $reduce = <<EOF;
function (key, values) {
  var total = 0;
  for (var i = 0; i < values.length; i++) {
    total += values[i];
  }
  return total;
}
EOF
$collection->insert({x => 1, tags => [qw(dog cat)]});
$collection->insert({x => 2, tags => ['cat']});
$collection->insert({x => 3, tags => [qw(mouse cat dog)]});
$collection->insert({x => 4, tags => []});
my $out
  = $collection->map_reduce($map, $reduce, {out => 'collection_test_results'});
$collection->drop;
$docs = $out->find->sort({value => -1})->all;
is_deeply $docs->[0], {_id => 'cat',   value => 3}, 'right document';
is_deeply $docs->[1], {_id => 'dog',   value => 2}, 'right document';
is_deeply $docs->[2], {_id => 'mouse', value => 1}, 'right document';
$out->drop;

# Perform map/reduce non-blocking
$collection->insert({x => 1, tags => [qw(dog cat)]});
$collection->insert({x => 2, tags => ['cat']});
$collection->insert({x => 3, tags => [qw(mouse cat dog)]});
$collection->insert({x => 4, tags => []});
($fail, $result) = ();
$collection->map_reduce(
  ($map, $reduce, {out => 'collection_test_results'}) => sub {
    my ($collection, $err, $out) = @_;
    $fail   = $err;
    $result = $out;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$mango->is_active, 'no operations in progress';
ok !$fail, 'no error';
$collection->drop;
$docs = $result->find->sort({value => -1})->all;
is_deeply $docs->[0], {_id => 'cat',   value => 3}, 'right document';
is_deeply $docs->[1], {_id => 'dog',   value => 2}, 'right document';
is_deeply $docs->[2], {_id => 'mouse', value => 1}, 'right document';
$result->drop;

# Perform inline map/reduce blocking
$collection->insert({x => 1, tags => [qw(dog cat)]});
$collection->insert({x => 2, tags => ['cat']});
$collection->insert({x => 3, tags => [qw(mouse cat dog)]});
$collection->insert({x => 4, tags => []});
$docs = $collection->map_reduce(bson_code($map), bson_code($reduce),
  {out => {inline => 1}});
$collection->drop;
is_deeply $docs->[0], {_id => 'cat',   value => 3}, 'right document';
is_deeply $docs->[1], {_id => 'dog',   value => 2}, 'right document';
is_deeply $docs->[2], {_id => 'mouse', value => 1}, 'right document';

# Perform inline map/reduce non-blocking
$collection->insert({x => 1, tags => [qw(dog cat)]});
$collection->insert({x => 2, tags => ['cat']});
$collection->insert({x => 3, tags => [qw(mouse cat dog)]});
$collection->insert({x => 4, tags => []});
($fail, $result) = ();
$collection->map_reduce(
  (bson_code($map), bson_code($reduce), {out => {inline => 1}}) => sub {
    my ($collection, $err, $docs) = @_;
    $fail   = $err;
    $result = $docs;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$mango->is_active, 'no operations in progress';
ok !$fail, 'no error';
$collection->drop;
is_deeply $result->[0], {_id => 'cat',   value => 3}, 'right document';
is_deeply $result->[1], {_id => 'dog',   value => 2}, 'right document';
is_deeply $result->[2], {_id => 'mouse', value => 1}, 'right document';

# Interrupted non-blocking remove
my $port = Mojo::IOLoop->generate_port;
$mango = Mango->new("mongodb://localhost:$port");
my $id = Mojo::IOLoop->server((port => $port) => sub { $_[1]->close });
($fail, $result) = ();
$mango->db->collection('collection_test')->remove(
  sub {
    my ($collection, $err, $num) = @_;
    $fail   = $err;
    $result = $num;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
Mojo::IOLoop->remove($id);
ok !$mango->is_active, 'no operations in progress';
like $fail, qr/Premature connection close/, 'right error';
ok !$result, 'remove was not successful';

done_testing();
