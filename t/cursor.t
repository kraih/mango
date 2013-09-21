use Mojo::Base -strict;

use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

use List::Util 'first';
use Mango;
use Mojo::IOLoop;

# Clean up before start
my $mango      = Mango->new($ENV{TEST_ONLINE});
my $collection = $mango->db->collection('cursor_test');
$collection->drop
  if first { $_ eq 'cursor_test' } @{$mango->db->collection_names};

# Add some documents to fetch
my $oids = $collection->insert([{test => 3}, {test => 1}, {test => 2}]);
is scalar @$oids, 3, 'three documents inserted';

# Fetch documents blocking
my $cursor = $collection->find->batch_size(2);
my @docs;
ok !$cursor->id, 'no cursor id';
push @docs, $cursor->next;
ok $cursor->id, 'cursor has id';
push @docs, $cursor->next;
push @docs, $cursor->next;
ok !$cursor->next, 'no more documents';
@docs = sort { $a->{test} <=> $b->{test} } @docs;
is $docs[0]{test}, 1, 'right document';
is $docs[1]{test}, 2, 'right document';
is $docs[2]{test}, 3, 'right document';

# Fetch all documents blocking
my $docs = $collection->find->batch_size(2)->all;
@$docs = sort { $a->{test} <=> $b->{test} } @$docs;
is $docs->[0]{test}, 1, 'right document';
is $docs->[1]{test}, 2, 'right document';
is $docs->[2]{test}, 3, 'right document';

# Fetch two documents blocking
$docs = $collection->find->limit(-2)->sort({test => 1})->all;
is scalar @$docs, 2, 'two documents';
is $docs->[0]{test}, 1, 'right document';
is $docs->[1]{test}, 2, 'right document';

# Build query
$cursor = $collection->find({test => 1});
is_deeply $cursor->build_query, {test => 1}, 'right query';
is_deeply $cursor->build_query(1), {'$query' => {test => 1}, '$explain' => 1},
  'right query';
$cursor->sort({test => -1});
is_deeply $cursor->build_query,
  {'$query' => {test => 1}, '$orderby' => {test => -1}}, 'right query';
$cursor->sort(undef)->hint({test => 1})->snapshot(1);
is_deeply $cursor->build_query,
  {'$query' => {test => 1}, '$hint' => {test => 1}, '$snapshot' => 1},
  'right query';
$cursor->hint(undef)->snapshot(undef)->max_scan(500);
is_deeply $cursor->build_query, {'$query' => {test => 1}, '$maxScan' => 500},
  'right query';

# Clone cursor
$cursor
  = $collection->find({test => {'$exists' => 1}})->batch_size(2)->limit(3)
  ->skip(1)->sort({test => 1})->fields({test => 1})->max_scan(100);
my $doc = $cursor->next;
ok defined $cursor->id, 'has a cursor id';
ok $doc->{test}, 'right document';
my $clone = $cursor->snapshot(1)->hint({test => 1})->tailable(1)->clone;
isnt $cursor, $clone, 'different objects';
ok !defined $clone->id, 'has no cursor id';
is $clone->batch_size, 2, 'right batch size';
is_deeply $clone->fields, {test => 1}, 'right fields';
is_deeply $clone->hint,   {test => 1}, 'right hint value';
is $clone->limit, 3, 'right limit';
is_deeply $clone->query, {test => {'$exists' => 1}}, 'right query';
is $clone->skip,     1,   'right skip value';
is $clone->snapshot, 1,   'right snapshot value';
is $clone->max_scan, 100, 'right max_scan value';
is $clone->tailable, 1,   'is tailable';
is_deeply $clone->sort, {test => 1}, 'right sort value';
$cursor = $collection->find({foo => 'bar'}, {foo => 1});
is_deeply $cursor->clone->query,  {foo => 'bar'}, 'right query';
is_deeply $cursor->clone->fields, {foo => 1},     'right fields';

# Explain blocking
$cursor = $collection->find({test => 2});
$doc = $cursor->explain;
is $doc->{n}, 1, 'one document';
$doc = $cursor->next;
is $doc->{test}, 2, 'right document';

# Explain non-blocking
$cursor = $collection->find({test => 2});
my ($fail, $result);
$cursor->explain(
  sub {
    my ($cursor, $err, $doc) = @_;
    $fail   = $err;
    $result = $doc->{n};
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$fail, 'no error';
is $result, 1, 'one document';
is $cursor->next->{test}, 2, 'right document';

# Get distinct values blocking
is_deeply [
  sort @{$collection->find({test => {'$gt' => 1}})->distinct('test')}
], [2, 3], 'right values';

# Get distinct values non-blocking
($fail, $result) = ();
$collection->find({test => {'$gt' => 1}})->distinct(
  test => sub {
    my ($cursor, $err, $values) = @_;
    $fail   = $err;
    $result = $values;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$fail, 'no error';
is_deeply [sort @$result], [2, 3], 'right values';

# Count documents blocking
is $collection->find({foo => 'bar'})->count, 0, 'no documents';
is $collection->find->skip(1)->limit(1)->count, 1, 'one document';
is $collection->find->count, 3, 'three documents';

# Count documents non-blocking
$fail = undef;
my @results;
my $delay = Mojo::IOLoop->delay(
  sub {
    my $delay = shift;
    $collection->find->count($delay->begin);
  },
  sub {
    my ($delay, $err, $count) = @_;
    $fail = $err;
    push @results, $count;
    $collection->find({foo => 'bar'})->count($delay->begin);
  },
  sub {
    my ($delay, $err, $count) = @_;
    $fail ||= $err;
    push @results, $count;
  }
);
$delay->wait;
ok !$fail, 'no error';
is_deeply \@results, [3, 0], 'right number of documents';

# Fetch documents non-blocking
$cursor = $collection->find->batch_size(2);
@docs   = ();
$fail   = undef;
$delay  = Mojo::IOLoop->delay(
  sub {
    my $delay = shift;
    $cursor->next($delay->begin);
  },
  sub {
    my ($delay, $err, $doc) = @_;
    $fail = $err;
    push @docs, $doc;
    $cursor->next($delay->begin);
  },
  sub {
    my ($delay, $err, $doc) = @_;
    $fail ||= $err;
    push @docs, $doc;
    $cursor->next($delay->begin);
  },
  sub {
    my ($delay, $err, $doc) = @_;
    $fail ||= $err;
    push @docs, $doc;
  }
);
$delay->wait;
ok !$fail, 'no error';
@docs = sort { $a->{test} <=> $b->{test} } @docs;
is $docs[0]{test}, 1, 'right document';
is $docs[1]{test}, 2, 'right document';
is $docs[2]{test}, 3, 'right document';

# Fetch all documents non-blocking
@docs = ();
$collection->find->batch_size(2)->all(
  sub {
    my ($collection, $err, $docs) = @_;
    @docs = @$docs;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
@docs = sort { $a->{test} <=> $b->{test} } @docs;
is $docs[0]{test}, 1, 'right document';
is $docs[1]{test}, 2, 'right document';
is $docs[2]{test}, 3, 'right document';

# Fetch subset of documents sorted
$docs = $collection->find->fields({_id => 0})->sort({test => 1})->all;
is_deeply $docs, [{test => 1}, {test => 2}, {test => 3}], 'right subset';

# Rewind cursor blocking
$cursor = $collection->find;
ok !$cursor->id, 'no cursor id';
$cursor->rewind;
$doc = $cursor->next;
ok $doc, 'found a document';
$cursor->rewind;
is_deeply $cursor->next, $doc, 'found same document again';

# Rewind cursor non-blocking
$fail   = undef;
@docs   = ();
$cursor = $collection->find;
$delay  = Mojo::IOLoop->delay(
  sub {
    my $delay = shift;
    $cursor->next($delay->begin);
  },
  sub {
    my ($delay, $err, $doc) = @_;
    $fail = $err;
    push @docs, $doc;
    $cursor->rewind($delay->begin);
  },
  sub {
    my ($delay, $err) = @_;
    $fail ||= $err;
    $cursor->next($delay->begin);
  },
  sub {
    my ($delay, $err, $doc) = @_;
    $fail ||= $err;
    push @docs, $doc;
  }
);
$delay->wait;
ok !$fail, 'no error';
is_deeply $docs[0], $docs[1], 'found same document again';

# Tailable cursor
$collection->drop;
$collection->create({capped => \1, max => 2, size => 100000});
my $collection2 = $mango->db->collection('cursor_test');
$collection2->insert([{test => 1}, {test => 2}]);
$cursor = $collection->find->tailable(1);
is $cursor->next->{test}, 1, 'right document';
is $cursor->next->{test}, 2, 'right document';
($fail, $result) = ();
my $tail;
$delay = Mojo::IOLoop->delay(
  sub {
    my $delay = shift;
    my $end   = $delay->begin;
    $cursor->next($delay->begin);
    Mojo::IOLoop->timer(
      0.5 => sub { $collection2->insert({test => 3} => $end) });
  },
  sub {
    my ($delay, $err1, $oid, $err2, $doc) = @_;
    $fail   = $err1 || $err2;
    $result = $oid;
    $tail   = $doc;
  }
);
$delay->wait;
ok !$fail, 'no error';
is $tail->{test}, 3, 'right document';
is $tail->{_id}, $result, 'same document';
$collection->drop;

done_testing();
