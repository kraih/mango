use Mojo::Base -strict;

use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

use Mango;
use Mango::BSON qw(bson_doc bson_oid);
use Mojo::IOLoop;

# Clean up before start
my $mango      = Mango->new($ENV{TEST_ONLINE});
my $collection = $mango->db->collection('bulk_test');
$collection->drop if $collection->options;

# Nothing blocking
my $results = $collection->bulk->execute;
is $results->{nInserted}, 0, 'no inserts';
is $results->{nMatched},  0, 'no matches';
is $results->{nModified}, 0, 'no modifications';
is $results->{nRemoved},  0, 'no removals';
is $results->{nUpserted}, 0, 'no upserts';
is_deeply $results->{upserted},           [], 'no upserts';
is_deeply $results->{writeConcernErrors}, [], 'no write concern errors';
is_deeply $results->{writeErrors},        [], 'no write errors';

# Nothing non-blocking
my ($fail, $result);
$collection->bulk->execute(
  sub {
    my ($bulk, $err, $results) = @_;
    $fail   = $err;
    $result = $results;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$fail, 'no error';
is $result->{nInserted}, 0, 'no inserts';
is $result->{nMatched},  0, 'no matches';
is $result->{nModified}, 0, 'no modifications';
is $result->{nRemoved},  0, 'no removals';
is $result->{nUpserted}, 0, 'no upserts';
is_deeply $result->{upserted},           [], 'no upserts';
is_deeply $result->{writeConcernErrors}, [], 'no write concern errors';
is_deeply $result->{writeErrors},        [], 'no write errors';

# Mixed bulk operations blocking
my $bulk = $collection->bulk;
ok $bulk->ordered, 'ordered bulk operations';
$bulk->insert({foo => 'bar'});
$bulk->find({foo => 'bar'})->update_one({foo => 'baz'});
$bulk->find({foo => 'yada'})->upsert->update_one({foo => 'baz'});
$bulk->find({foo => 'baz'})->remove;
$results = $bulk->execute;
is $results->{nInserted}, 1, 'one insert';
is $results->{nMatched},  1, 'one match';
is $results->{nModified}, 2, 'two modifications';
is $results->{nRemoved},  2, 'two removals';
is $results->{nUpserted}, 1, 'one upsert';
ok $results->{upserted}[0], 'one upsert';
is_deeply $results->{writeConcernErrors}, [], 'no write concern errors';
is_deeply $results->{writeErrors},        [], 'no write errors';

# Mixed bulk operations non-blocking
$bulk = $collection->bulk;
$bulk->insert({foo => 'bar'});
$bulk->find({foo => 'bar'})->update_one({foo => 'baz'});
$bulk->find({foo => 'yada'})->upsert->update_one({foo => 'baz'});
$bulk->find({foo => 'baz'})->remove;
($fail, $result) = ();
$bulk->execute(
  sub {
    my ($bulk, $err, $results) = @_;
    $fail   = $err;
    $result = $results;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$fail, 'no error';
is $result->{nInserted}, 1, 'one insert';
is $result->{nMatched},  1, 'one match';
is $result->{nModified}, 2, 'two modifications';
is $result->{nRemoved},  2, 'two removals';
is $result->{nUpserted}, 1, 'one upsert';
ok $result->{upserted}[0], 'one upsert';
is_deeply $result->{writeConcernErrors}, [], 'no write concern errors';
is_deeply $result->{writeErrors},        [], 'no write errors';

# All operations
$bulk = $collection->bulk;
$bulk->insert({foo => 'a'})->insert({foo => 'b'})->insert({foo => 'c'});
$bulk->find({foo => {'$exists' => 1}})->update_one({foo => 'd'});
$results = $bulk->execute;
is $results->{nInserted}, 3, 'three inserts';
is $results->{nModified}, 1, 'one modification';
$bulk = $collection->bulk;
$bulk->find({foo => {'$exists' => 1}})->remove_one;
$bulk->find({foo => {'$exists' => 1}})->update({'$set' => {foo => 'a'}});
$results = $bulk->execute;
is $results->{nModified}, 2, 'two modifications';
is $results->{nRemoved},  1, 'one removal';
$results = $collection->bulk->find->remove->execute;
is $results->{nRemoved}, 2, 'two removals';

# Split up large documents into multiple operations (many documents)
is $mango->max_write_batch_size, 1000, 'right value';
$bulk = $collection->bulk;
$bulk->insert({foo => $_}) for 1 .. 1001;
$results = $bulk->execute;
is $results->{nInserted}, 1001, 'over one thousand inserts';

# Split up large documents into multiple operations (large documents)
is $mango->max_bson_size, 16777216, 'right value';
my $large = 'x' x 5242880;
$bulk = $collection->bulk;
$bulk->insert({foo => $large}) for 1 .. 5;
$results = $bulk->execute;
is $results->{nInserted}, 5, 'five inserts';

# Insert the same document twice blocking (separated by update)
my $doc = bson_doc _id => bson_oid, foo => 'bar';
$bulk = $collection->bulk->insert($doc)->find({foo => 'bar'})
  ->update_one({'$set' => {foo => 'baz'}})->insert($doc);
eval { $bulk->execute };
like $@, qr/^Write error at index 2: .+/, 'right error';

# Insert the same document twice non-blocking (separated by update)
$doc = bson_doc _id => bson_oid, foo => 'bar';
$bulk = $collection->bulk->insert($doc)->find({foo => 'bar'})
  ->update_one({'$set' => {foo => 'baz'}})->insert($doc);
($fail, $result) = ();
$bulk->execute(
  sub {
    my ($bulk, $err, $results) = @_;
    $fail   = $err;
    $result = $results;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
like $fail, qr/^Write error at index 2: .+/, 'right error';
is $result->{nInserted}, 1, 'one insert';

# Insert the same document three times blocking (unordered)
$doc = bson_doc _id => bson_oid, foo => 'bar';
$bulk = $collection->bulk->insert($doc)->insert($doc)->insert($doc);
eval { $bulk->ordered(0)->execute };
like $@, qr/Write error at index 1: .+/, 'right error';
like $@, qr/Write error at index 2: .+/, 'right error';

done_testing();
