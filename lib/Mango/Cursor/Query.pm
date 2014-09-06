package Mango::Cursor::Query;
use Mojo::Base 'Mango::Cursor';

use Mango::BSON 'bson_doc';

has [
  qw(await_data comment hint max_scan max_time_ms read_preference snapshot),
  qw(sort tailable)
];
has [qw(fields query)] => sub { {} };
has skip => 0;

sub build_query {
  my ($self, $explain) = @_;

  my %ext;
  if (my $comment = $self->comment) { $ext{'$comment'} = $comment }
  if ($explain) { $ext{'$explain'} = 1 }
  if (my $hint     = $self->hint)        { $ext{'$hint'}      = $hint }
  if (my $max_scan = $self->max_scan)    { $ext{'$maxScan'}   = $max_scan }
  if (my $max      = $self->max_time_ms) { $ext{'$maxTimeMS'} = $max }
  if (my $pref = $self->read_preference) { $ext{'$readPreference'} = $pref }
  if (my $snapshot = $self->snapshot) { $ext{'$snapshot'} = 1 }
  if (my $sort     = $self->sort)     { $ext{'$orderby'}  = $sort }

  my $query = $self->query;
  return $query unless keys %ext;
  return bson_doc $query->{'$query'} ? %$query : ('$query' => $query), %ext;
}

sub clone {
  my $self  = shift;
  my $clone = $self->new;
  $clone->$_($self->$_) for qw(await_data batch_size collection comment);
  $clone->$_($self->$_) for qw(fields hint limit max_scan max_time_ms query);
  $clone->$_($self->$_) for qw(read_preference skip snapshot sort tailable);
  return $clone;
}

sub count {
  my $self = shift;
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;

  my $collection = $self->collection;
  my $command    = bson_doc
    count => $collection->name,
    query => $self->build_query,
    skip  => $self->skip,
    limit => $self->limit;

  # Non-blocking
  return $collection->db->command(
    $command => sub {
      my ($collection, $err, $doc) = @_;
      $self->$cb($err, $doc ? $doc->{n} : 0);
    }
  ) if $cb;

  # Blocking
  my $doc = $collection->db->command($command);
  return $doc ? $doc->{n} : 0;
}

sub distinct {
  my ($self, $key) = (shift, shift);
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;

  my $collection = $self->collection;
  my $command    = bson_doc
    distinct => $collection->name,
    key      => $key,
    query    => $self->build_query;

  # Blocking
  my $db = $collection->db;
  return $db->command($command)->{values} unless $cb;

  # Non-blocking
  $db->command($command => sub { shift; $self->$cb(shift, shift->{values}) });
}

sub explain {
  my ($self, $cb) = @_;

  # Non-blocking
  my $clone = $self->clone->query($self->build_query(1))->sort(undef);
  return $clone->next(sub { shift; $self->$cb(@_) }) if $cb;

  # Blocking
  return $clone->next;
}

sub _start {
  my ($self, $cb) = @_;

  my $collection = $self->collection;
  my $name       = $collection->full_name;
  my $flags      = {};
  $flags->{tailable_cursor} = 1 if $self->tailable;
  $flags->{await_data}      = 1 if $self->await_data;
  my @query = (
    $name, $flags, $self->skip, $self->num_to_return, $self->build_query,
    $self->fields
  );

  # Non-blocking
  return $collection->db->mango->query(
    @query => sub { shift; $self->$cb(shift, $self->_enqueue(shift)) })
    if $cb;

  # Blocking
  return $self->_enqueue($collection->db->mango->query(@query));
}

1;

=encoding utf8

=head1 NAME

Mango::Cursor::Query - MongoDB query cursor

=head1 SYNOPSIS

  use Mango::Cursor::Query;

  my $cursor = Mango::Cursor::Query->new(collection => $collection);
  my $docs   = $cursor->all;

=head1 DESCRIPTION

L<Mango::Cursor::Query> is a container for MongoDB query cursors used by
L<Mango::Collection>.

=head1 ATTRIBUTES

L<Mango::Cursor::Query> inherits all attributes from L<Mango::Cursor> and
implements the following new ones.

=head2 await_data

  my $await = $cursor->await_data;
  $cursor   = $cursor->await_data(1);

Await data.

=head2 comment

  my $comment = $cursor->comment;
  $cursor     = $cursor->comment('Fun query!');

A comment to identify query.

=head2 fields

  my $fields = $cursor->fields;
  $cursor    = $cursor->fields({foo => 1});

Select fields from documents.

=head2 hint

  my $hint = $cursor->hint;
  $cursor  = $cursor->hint({foo => 1});

Force a specific index to be used.

=head2 max_scan

  my $max = $cursor->max_scan;
  $cursor = $cursor->max_scan(500);

Limit the number of documents to scan.

=head2 max_time_ms

  my $max = $cursor->max_time_ms;
  $cursor = $cursor->max_time_ms(500);

Timeout for query in milliseconds.

=head2 query

  my $query = $cursor->query;
  $cursor   = $cursor->query({foo => 'bar'});

Original query.

=head2 read_preference

  my $pref = $cursor->read_preference;
  $cursor  = $cursor->read_preference({mode => 'SECONDARY'});

Read preference.

=head2 skip

  my $skip = $cursor->skip;
  $cursor  = $cursor->skip(5);

Number of documents to skip, defaults to C<0>.

=head2 snapshot

  my $snapshot = $cursor->snapshot;
  $cursor      = $cursor->snapshot(1);

Use snapshot mode.

=head2 sort

  my $sort = $cursor->sort;
  $cursor  = $cursor->sort({foo => 1});
  $cursor  = $cursor->sort(bson_doc(foo => 1, bar => -1));

Sort documents, the order of keys matters.

=head2 tailable

  my $tailable = $cursor->tailable;
  $cursor      = $cursor->tailable(1);

Tailable cursor.

=head1 METHODS

L<Mango::Cursor::Query> inherits all methods from L<Mango::Cursor> and
implements the following new ones.

=head2 build_query

  my $query = $cursor->build_query;
  my $query = $cursor->build_query($explain);

Generate final query with cursor attributes.

=head2 clone

  my $clone = $cursor->clone;

Clone cursor.

=head2 count

  my $count = $cursor->count;

Count number of documents this cursor can return. You can also append a
callback to perform operation non-blocking.

  $cursor->count(sub {
    my ($cursor, $err, $count) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 distinct

  my $values = $cursor->distinct('foo');

Get all distinct values for key. You can also append a callback to perform
operation non-blocking.

  $cursor->distinct(foo => sub {
    my ($cursor, $err, $values) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 explain

  my $doc = $cursor->explain;

Provide information on the query plan. You can also append a callback to
perform operation non-blocking.

  $cursor->explain(sub {
    my ($cursor, $err, $doc) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head1 SEE ALSO

L<Mango>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
