use Mojo::Base -strict;

use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

use Mango;
use Mango::BSON qw(bson_code bson_doc bson_oid bson_true);
use Mojo::IOLoop;

# Clean up before start
my $mango      = Mango->new($ENV{TEST_ONLINE});
my $collection = $mango->db->collection('collection_test');
$collection->drop if $collection->options;

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
ok !$fail, 'no error';
is $result->{count}, 2, 'right number of documents';

# Update documents blocking
my $oid = bson_oid;
is $collection->update($oid, {foo => 'bar'})->{n}, 0, 'upsert is 0 by default';
is $collection->update($oid, {foo => 'bar'}, {upsert => 1})->{n}, 1,
  '1 document created';
is $collection->update($oid, {foo => 'works'})->{n}, 1, '1 document updated';
is $collection->find_one($oid)->{foo}, 'works', 'right value';
is $collection->remove($oid)->{n},     1,       'one doc removed';

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
my $oid = bson_oid;
is $collection->update($oid, {foo => 'bar'})->{n}, 0, 'no documents updated';
is $collection->update($oid, {foo => 'bar'}, {upsert => 1})->{n}, 1,
  'one document updated';
is $collection->update($oid, {foo => 'works'})->{n}, 1, 'one document updated';
is $collection->find_one($oid)->{foo}, 'works', 'right value';
is $collection->remove($oid)->{n},     1,       'one document removed';

# Remove one document blocking
is $collection->remove({foo => 'baz'})->{n}, 1, 'one document removed';
ok $collection->find_one($oids->[0]), 'document still exists';
ok !$collection->find_one($oids->[1]), 'no document';
is $collection->remove->{n}, 1, 'one document removed';
ok !$collection->find_one($oids->[0]), 'no document';

# Find and modify document blocking
$oid = $collection->insert({atomic => 1});
is $collection->find_one($oid)->{atomic}, 1, 'right document';
my $doc = $collection->find_and_modify(
  {query => {atomic => 1}, update => {'$set' => {atomic => 2}}});
is $doc->{atomic}, 1, 'right document';
is $collection->find_one($oid)->{atomic}, 2, 'right document';
is $collection->remove({atomic => 2})->{n}, 1, 'removed one document';

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
ok !$fail, 'no error';
is $result->{atomic}, 1, 'right document';
is $collection->find_one($oid)->{atomic}, 2, 'right document';
is $collection->remove({atomic => 2})->{n}, 1, 'removed one document';

# Get options blocking
is $collection->options->{name}, $collection->full_name, 'right name';

# Get options non-blocking
($fail, $result) = ();
$collection->options(
  sub {
    my ($collection, $err, $doc) = @_;
    $fail   = $err;
    $result = $doc;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$fail, 'no error';
is $result->{name}, $collection->full_name, 'right name';

# Get options blocking (missing collection)
is $mango->db->collection('collection_test2')->options, undef,
  'collection does not exist';

# Get options non-blocking (missing collection)
($fail, $result) = ();
$mango->db->collection('collection_test2')->options(
  sub {
    my ($collection, $err, $doc) = @_;
    $fail   = $err;
    $result = $doc;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$fail, 'no error';
is $result, undef, 'collection does not exist';

# Aggregate collection blocking
$collection->insert([{more => 1}, {more => 2}, {more => 3}]);
my $cursor = $collection->aggregate(
  [{'$group' => {_id => undef, total => {'$sum' => '$more'}}}]);
ok !$cursor->id, 'no cursor id';
is $cursor->next->{total}, 6, 'right result';
is $collection->remove({more => {'$exists' => 1}})->{n}, 3,
  'three documents removed';

# Aggregate collection non-blocking
$collection->insert([{more => 1}, {more => 2}, {more => 3}]);
($fail, $result) = ();
$collection->aggregate(
  [{'$group' => {_id => undef, total => {'$sum' => '$more'}}}] => sub {
    my ($collection, $err, $cursor) = @_;
    $fail   = $err;
    $result = $cursor;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$fail, 'no error';
is $result->next->{total}, 6, 'right result';
is $collection->remove({more => {'$exists' => 1}})->{n}, 3,
  'three documents removed';

# Explain aggregation
$collection->insert({stuff => $_}) for 1 .. 30;
$doc = $collection->aggregate([{'$match' => {stuff => {'$gt' => 0}}}],
  {explain => \1});
ok $doc->{stages}, 'right result';
is $collection->remove->{n}, 30, 'thirty documents removed';

# Aggregate with collections
$collection->insert({stuff => $_}) for 1 .. 30;
my $out = $collection->aggregate(
  [
    {'$match' => {stuff => {'$gt' => 0}}},
    {'$out'   => 'collection_test_results'}
  ]
);
is $out->name, 'collection_test_results', 'right name';
is $out->find->count, 30, 'thirty documents found';
$out->drop;
is $collection->remove->{n}, 30, 'thirty documents removed';

# Aggregate with cursor blocking (multiple batches)
$collection->insert({stuff => $_}) for 1 .. 30;
$cursor = $collection->aggregate([{'$match' => {stuff => {'$gt' => 0}}}],
  {cursor => {batchSize => 5}});
ok $cursor->id, 'cursor has id';
is scalar @{$cursor->all}, 30, 'thirty documents found';
is $collection->remove->{n}, 30, 'thirty documents removed';

# Aggregate with cursor non-blocking (multiple batches)
$collection->insert({stuff => $_}) for 1 .. 30;
($fail, $result) = ();
my $delay = Mojo::IOLoop->delay(
  sub {
    my $delay = shift;
    $collection->aggregate(
      [{'$match' => {stuff => {'$gt' => 0}}}],
      {cursor => {batchSize => 5}},
      $delay->begin
    );
  },
  sub {
    my ($delay, $err, $cursor) = @_;
    return $delay->pass($err) if $err;
    $cursor->all($delay->begin);
  },
  sub {
    my ($delay, $err, $docs) = @_;
    $fail   = $err;
    $result = $docs;
  }
);
$delay->wait;
is scalar @$result, 30, 'thirty documents found';
is $collection->remove->{n}, 30, 'thirty documents removed';

# Save document blocking
$oid = $collection->save({update => 'me'});
$doc = $collection->find_one($oid);
is $doc->{update}, 'me', 'right document';
$doc->{update} = 'too';
is $collection->save($doc), $oid, 'same object id';
$doc = $collection->find_one($oid);
is $doc->{update}, 'too', 'right document';
is $collection->remove($oid)->{n}, 1, 'one document removed';
$oid = bson_oid;
$doc = bson_doc _id => $oid, save => 'me';
is $collection->save($doc), $oid, 'same object id';
$doc = $collection->find_one($oid);
is $doc->{save}, 'me', 'right document';
is $collection->remove({_id => $oid})->{n}, 1, 'one document removed';

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
ok !$fail, 'no error';
is $oid, $result, 'same object id';
$doc = $collection->find_one($oid);
is $doc->{update}, 'too', 'right document';
is $collection->remove($oid)->{n}, 1, 'one document removed';
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
ok !$fail, 'no error';
is $oid, $result, 'same object id';
$doc = $collection->find_one($oid, {_id => 0});
is_deeply $doc, {save => 'me'}, 'right document';
is $collection->remove($oid)->{n}, 1, 'one document removed';

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
ok !$fail, 'no error';
ok !$collection->find_one($oid), 'no document';

# Ensure and drop index blocking
$collection->insert({test => 23, foo => 'bar'});
$collection->ensure_index({test => 1}, {unique => \1});
is $collection->find->count, 1, 'one document';
is $collection->index_information->{test}{unique}, bson_true,
  'index is unique';
$collection->drop_index('test');
is $collection->index_information->{test}, undef, 'no index';
$collection->drop;

# Ensure and drop index non-blocking
$collection->insert({test => 23, foo => 'bar'});
($fail, $result) = ();
$delay = Mojo::IOLoop->delay(
  sub {
    my $delay = shift;
    $collection->ensure_index(({test => 1}, {unique => \1}) => $delay->begin);
  },
  sub {
    my ($delay, $err) = @_;
    return $delay->pass($err) if $err;
    $collection->index_information($delay->begin);
  },
  sub {
    my ($delay, $err, $info) = @_;
    $fail   = $err;
    $result = $info;
  }
);
$delay->wait;
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
    return $delay->pass($err) if $err;
    $collection->index_information($delay->begin);
  },
  sub {
    my ($delay, $err, $info) = @_;
    $fail   = $err;
    $result = $info;
  }
);
$delay->wait;
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
$out
  = $collection->map_reduce($map, $reduce, {out => 'collection_test_results'});
$collection->drop;
my $docs = $out->find->sort({value => -1})->all;
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
ok !$fail, 'no error';
$collection->drop;
is_deeply $result->[0], {_id => 'cat',   value => 3}, 'right document';
is_deeply $result->[1], {_id => 'dog',   value => 2}, 'right document';
is_deeply $result->[2], {_id => 'mouse', value => 1}, 'right document';

# Insert same document twice blocking
$doc = bson_doc _id => bson_oid, foo => 'bar';
$collection->insert($doc);
eval { $collection->insert($doc) };
like $@, qr/^Write error at index 0: .+/, 'right error';
$collection->drop;

# Insert same document twice non-blocking
$doc = bson_doc _id => bson_oid, foo => 'bar';
$collection->insert($doc);
$fail = undef;
$collection->insert(
  $doc => sub {
    my ($collection, $err) = @_;
    $fail = $err;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
like $fail, qr/^Write error at index 0: .+/, 'right error';

# Insert same document twice blocking (upsert)
$doc = bson_doc _id => bson_oid, foo => 'bar';
$collection->insert($doc);
eval { $collection->update({foo => 'baz'}, $doc, {upsert => 1}) };
like $@, qr/^Write error at index 0: .+/, 'right error';
$collection->drop;

# Insert same document twice non-blocking (upsert)
$doc = bson_doc _id => bson_oid, foo => 'bar';
$collection->insert($doc);
$fail = undef;
$collection->update(
  {foo => 'baz'} => $doc => {upsert => 1} => sub {
    my ($collection, $err) = @_;
    $fail = $err;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
like $fail, qr/^Write error at index 0: .+/, 'right error';

# Interrupted non-blocking remove
my $id
  = Mojo::IOLoop->server((address => '127.0.0.1') => sub { $_[1]->close });
my $port = Mojo::IOLoop->acceptor($id)->handle->sockport;
$mango = Mango->new("mongodb://localhost:$port");
($fail, $result) = ();
$mango->db->collection('collection_test')->remove(
  sub {
    my ($collection, $err, $doc) = @_;
    $fail   = $err;
    $result = $doc;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
Mojo::IOLoop->remove($id);
like $fail, qr/Premature connection close/, 'right error';
ok !$result->{n}, 'remove was not successful';

done_testing();
