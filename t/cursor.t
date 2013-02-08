use Mojo::Base -strict;

use Test::More;
use Mango;
use Mojo::IOLoop;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

# Add some documents to fetch
my $mango      = Mango->new($ENV{TEST_ONLINE});
my $collection = $mango->db->collection('cursor_test');
$collection->remove;
my $oids = $collection->insert([{test => 3}, {test => 1}, {test => 2}]);
is scalar @$oids, 3, 'three documents inserted';

# Fetch documents blocking
my $cursor = $collection->find({})->batch_size(2);
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
my $docs = $collection->find({})->batch_size(2)->all;
@$docs = sort { $a->{test} <=> $b->{test} } @$docs;
is $docs->[0]{test}, 1, 'right document';
is $docs->[1]{test}, 2, 'right document';
is $docs->[2]{test}, 3, 'right document';

# Fetch two documents blocking
$docs = $collection->find({})->limit(2)->sort({test => 1})->all;
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

# Clone cursor
$cursor
  = $collection->find({test => {'$exists' => 1}})->batch_size(2)->limit(3)
  ->skip(1)->sort({test => 1})->fields({test => 1});
my $doc = $cursor->next;
ok defined $cursor->id, 'has a cursor id';
ok $doc->{test}, 'right document';
my $clone = $cursor->snapshot(1)->hint({test => 1})->clone;
isnt $cursor, $clone, 'different objects';
ok !defined $clone->id, 'has no cursor id';
is $clone->batch_size, 2, 'right batch size';
is_deeply $clone->fields, {test => 1}, 'right fields';
is_deeply $clone->hint,   {test => 1}, 'right hint value';
is $clone->limit, 3, 'right limit';
is_deeply $clone->query, {test => {'$exists' => 1}}, 'right query';
is $clone->skip,     1, 'right skip value';
is $clone->snapshot, 1, 'right snapshot value';
is_deeply $clone->sort, {test => 1}, 'right sort value';

# Explain blocking
$cursor = $collection->find({test => 2});
$doc = $cursor->explain;
is $doc->{n}, 1, 'one document';
$doc = $cursor->next;
is $doc->{test}, 2, 'right document';

# Explain non-blocking
$cursor = $collection->find({test => 2});
my ($fail, $n, $test);
my $delay = Mojo::IOLoop->delay(
  sub {
    my $delay = shift;
    $cursor->explain($delay->begin);
  },
  sub {
    my ($delay, $err, $doc) = @_;
    $fail = $err;
    $n    = $doc->{n};
    $cursor->next($delay->begin);
  },
  sub {
    my ($delay, $err, $doc) = @_;
    $fail ||= $err;
    $test = $doc->{test};
  }
);
$delay->wait;
ok !$mango->is_active, 'no operations in progress';
ok !$fail, 'no error';
is $n,    1, 'one document';
is $test, 2, 'right document';

# Count documents blocking
is $collection->find({foo => 'bar'})->count, 0, 'no documents';
is $collection->find({})->skip(1)->limit(1)->count, 1, 'one document';
is $collection->find({})->count, 3, 'three documents';

# Count documents non-blocking
$fail = undef;
my @count;
$delay = Mojo::IOLoop->delay(
  sub {
    my $delay = shift;
    $collection->find({})->count($delay->begin);
  },
  sub {
    my ($delay, $err, $count) = @_;
    $fail = $err;
    push @count, $count;
    $collection->find({foo => 'bar'})->count($delay->begin);
  },
  sub {
    my ($delay, $err, $count) = @_;
    $fail ||= $err;
    push @count, $count;
  }
);
$delay->wait;
ok !$mango->is_active, 'no operations in progress';
ok !$fail, 'no error';
is_deeply \@count, [3, 0], 'right number of documents';

# Fetch documents non-blocking
$cursor = $collection->find({})->batch_size(2);
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
ok !$mango->is_active, 'no operations in progress';
ok !$fail, 'no error';
@docs = sort { $a->{test} <=> $b->{test} } @docs;
is $docs[0]{test}, 1, 'right document';
is $docs[1]{test}, 2, 'right document';
is $docs[2]{test}, 3, 'right document';

# Fetch all documents non-blocking
@docs = ();
$collection->find({})->batch_size(2)->all(
  sub {
    my ($collection, $err, $docs) = @_;
    @docs = @$docs;
    Mojo::IOLoop->stop;
  }
);
Mojo::IOLoop->start;
ok !$mango->is_active, 'no operations in progress';
@docs = sort { $a->{test} <=> $b->{test} } @docs;
is $docs[0]{test}, 1, 'right document';
is $docs[1]{test}, 2, 'right document';
is $docs[2]{test}, 3, 'right document';

# Fetch subset of documents sorted
$docs = $collection->find({})->fields({_id => 0})->sort({test => 1})->all;
is_deeply $docs, [{test => 1}, {test => 2}, {test => 3}], 'right subset';

# Rewind cursor blocking
$cursor = $collection->find({});
ok !$cursor->id, 'no cursor id';
$cursor->rewind;
$doc = $cursor->next;
ok $doc, 'found a document';
$cursor->rewind;
is_deeply $cursor->next, $doc, 'found same document again';

# Rewind cursor non-blocking
$fail   = undef;
@docs   = ();
$cursor = $collection->find({});
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
  },
);
$delay->wait;
ok !$mango->is_active, 'no operations in progress';
ok !$fail, 'no error';
is_deeply $docs[0], $docs[1], 'found same document again';

# Remove all documents from collection
is $collection->remove->{n}, 3, 'three documents removed';

done_testing();
