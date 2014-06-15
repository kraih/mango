package Mango::Collection;
use Mojo::Base -base;

use Carp 'croak';
use Mango::BSON qw(bson_code bson_doc bson_oid);
use Mango::Bulk;
use Mango::Cursor;

has [qw(db name)];

sub aggregate {
  my ($self, $pipeline) = (shift, shift);
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;

  my $command = bson_doc(aggregate => $self->name, pipeline => $pipeline,
    %{shift // {}});
  $command->{cursor} //= {} unless $command->{explain};

  # Blocking
  return $self->_aggregate($command, $self->db->command($command)) unless $cb;

  # Non-blocking
  return $self->db->command($command,
    sub { shift; $self->$cb(shift, $self->_aggregate($command, shift)) });
}

sub build_index_name { join '_', keys %{$_[1]} }

sub bulk { Mango::Bulk->new(collection => shift) }

sub create {
  my $self = shift;
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;
  return $self->_command(bson_doc(create => $self->name, %{shift // {}}), $cb);
}

sub drop { $_[0]->_command(bson_doc(drop => $_[0]->name), $_[1]) }

sub drop_index {
  my ($self, $name) = (shift, shift);
  return $self->_command(bson_doc(dropIndexes => $self->name, index => $name),
    shift);
}

sub ensure_index {
  my ($self, $spec) = (shift, shift);
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;
  my $doc = shift // {};

  $doc->{name} //= $self->build_index_name($spec);
  $doc->{key} = $spec;

  # Non-blocking
  my $command = bson_doc createIndexes => $self->name, indexes => [$doc];
  return $self->db->command($command => sub { shift; $self->$cb(shift) })
    if $cb;

  # Blocking
  $self->db->command($command);
}

sub find {
  Mango::Cursor->new(
    collection => shift,
    query      => shift // {},
    fields     => shift // {}
  );
}

sub find_and_modify {
  my ($self, $opts) = (shift, shift);
  return $self->_command(bson_doc(findAndModify => $self->name, %$opts),
    shift, sub { shift->{value} });
}

sub find_one {
  my ($self, $query) = (shift, shift);
  $query = {_id => $query} if ref $query eq 'Mango::BSON::ObjectID';
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;

  # Non-blocking
  my $cursor = $self->find($query, @_)->limit(-1);
  return $cursor->next(sub { shift; $self->$cb(@_) }) if $cb;

  # Blocking
  return $cursor->next;
}

sub full_name { join '.', $_[0]->db->name, $_[0]->name }

sub index_information {
  my ($self, $cb) = @_;

  # Non-blocking
  my $collection = $self->db->collection('system.indexes');
  my $cursor = $collection->find({ns => $self->full_name})->fields({ns => 0});
  return $cursor->all(sub { shift; $self->$cb(shift, _indexes(shift)) })
    if $cb;

  # Blocking
  return _indexes($cursor->all);
}

sub insert {
  my ($self, $docs) = @_;
  $docs = [$docs] unless ref $docs eq 'ARRAY';
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;

  # Make sure all documents have ids
  my @ids = map { $_->{_id} //= bson_oid } @$docs;
  my $command = bson_doc
    insert       => $self->name,
    documents    => $docs,
    ordered      => \1,
    writeConcern => $self->db->build_write_concern;

  return $self->_command($command, $cb, sub { @ids > 1 ? \@ids : $ids[0] });
}

sub map_reduce {
  my ($self, $map, $reduce) = (shift, shift, shift);
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;
  my $command = bson_doc
    mapreduce => $self->name,
    map       => ref $map ? $map : bson_code($map),
    reduce    => ref $reduce ? $reduce : bson_code($reduce),
    %{shift // {}};

  # Blocking
  return $self->_map_reduce($self->db->command($command)) unless $cb;

  # Non-blocking
  return $self->db->command(
    $command => sub { shift; $self->$cb(shift, $self->_map_reduce(shift)) });
}

sub options {
  my ($self, $cb) = @_;

  # Non-blocking
  my $query = {name => $self->full_name};
  my $namespaces = $self->db->collection('system.namespaces');
  return $namespaces->find_one($query => sub { shift; $self->$cb(@_) }) if $cb;

  # Blocking
  return $namespaces->find_one($query);
}

sub remove {
  my $self  = shift;
  my $cb    = ref $_[-1] eq 'CODE' ? pop : undef;
  my $query = shift // {};
  my $flags = shift // {};

  ($query, $flags) = ({_id => $query}, {single => 1})
    if ref $query eq 'Mango::BSON::ObjectID';
  my $command = bson_doc
    delete       => $self->name,
    deletes      => [{q => $query, limit => $flags->{single} ? 1 : 0}],
    ordered      => \1,
    writeConcern => $self->db->build_write_concern;

  return $self->_command($command, $cb);
}

sub save {
  my ($self, $doc, $cb) = @_;

  # New document
  return $self->insert($doc, $cb) unless $doc->{_id};

  # Update non-blocking
  my @update = ({_id => $doc->{_id}}, $doc, {upsert => 1});
  return $self->update(@update => sub { shift->$cb(shift, $doc->{_id}) })
    if $cb;

  # Update blocking
  $self->update(@update);
  return $doc->{_id};
}

sub stats { $_[0]->_command(bson_doc(collstats => $_[0]->name), $_[1]) }

sub update {
  my ($self, $query, $update) = (shift, shift, shift);
  my $cb = ref $_[-1] eq 'CODE' ? pop : undef;
  my $flags = shift // {};

  $update = {
    q      => $query,
    u      => $update,
    upsert => $flags->{upsert} ? \1 : \0,
    multi  => $flags->{multi} ? \1 : \0
  };
  my $command = bson_doc
    update       => $self->name,
    updates      => [$update],
    ordered      => \1,
    writeConcern => $self->db->build_write_concern;

  return $self->_command($command, $cb);
}

sub _aggregate {
  my ($self, $command, $doc) = @_;

  # Document (explain)
  return $doc if $command->{explain};

  # Collection
  my $out = $command->{pipeline}[-1]{'$out'};
  return $self->db->collection($out) if defined $out;

  # Cursor
  my $cursor = $doc->{cursor};
  return Mango::Cursor->new(collection => $self, id => $cursor->{id})
    ->add_batch($cursor->{firstBatch});
}

sub _command {
  my ($self, $command, $cb, $return) = @_;
  $return ||= sub {shift};

  # Non-blocking
  my $db       = $self->db;
  my $protocol = $db->mango->protocol;
  return $db->command(
    $command => sub {
      my ($db, $err, $doc) = @_;
      $err ||= $protocol->write_error($doc);
      $self->$cb($err, $return->($doc));
    }
  ) if $cb;

  # Blocking
  my $doc = $db->command($command);
  if (my $err = $protocol->write_error($doc)) { croak $err }
  return $return->($doc);
}

sub _indexes {
  my $indexes = bson_doc;
  if (my $docs = shift) { $indexes->{delete $_->{name}} = $_ for @$docs }
  return $indexes;
}

sub _map_reduce {
  my ($self, $doc) = @_;
  return $doc->{results} unless $doc->{result};
  return $self->db->collection($doc->{result});
}

1;

=encoding utf8

=head1 NAME

Mango::Collection - MongoDB collection

=head1 SYNOPSIS

  use Mango::Collection;

  my $collection = Mango::Collection->new(db => $db);
  my $cursor     = $collection->find({foo => 'bar'});

=head1 DESCRIPTION

L<Mango::Collection> is a container for MongoDB collections used by
L<Mango::Database>.

=head1 ATTRIBUTES

L<Mango::Collection> implements the following attributes.

=head2 db

  my $db      = $collection->db;
  $collection = $collection->db(Mango::Database->new);

L<Mango::Database> object this collection belongs to.

=head2 name

  my $name    = $collection->name;
  $collection = $collection->name('bar');

Name of this collection.

=head1 METHODS

L<Mango::Collection> inherits all methods from L<Mojo::Base> and implements
the following new ones.

=head2 aggregate

  my $cursor = $collection->aggregate(
    [{'$group' => {_id => undef, total => {'$sum' => '$foo'}}}]);
  my $collection = $collection->aggregate(
    [{'$match' => {'$gt' => 23}}, {'$out' => 'some_collection'}]);
  my $doc = $collection->aggregate(
    [{'$match' => {'$gt' => 23}}], {explain => bson_true});

Aggregate collection with aggregation framework, additional options will be
passed along to the server verbatim. You can also append a callback to perform
operation non-blocking.

  my $pipeline = [{'$group' => {_id => undef, total => {'$sum' => '$foo'}}}];
  $collection->aggregate($pipeline => sub {
    my ($collection, $err, $cursor) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 build_index_name

  my $name = $collection->build_index_name(bson_doc(foo => 1, bar => -1));
  my $name = $collection->build_index_name({foo => 1});

Build name for index specification, the order of keys matters for compound
indexes.

=head2 bulk

  my $bulk = $collection->bulk;

Build L<Mango::Bulk> object.

  my $bulk = $collection->bulk;
  $bulk->insert({foo => $_}) for 1 .. 10;
  $bulk->find({foo => 4})->update_one({'$set' => {bar => 'baz'}});
  $bulk->find({foo => 7})->remove_one;
  my $results = $bulk->execute;

=head2 create

  $collection->create;
  $collection->create({capped => bson_true, max => 5, size => 10000});

Create collection. You can also append a callback to perform operation
non-blocking.

  $collection->create({capped => bson_true, max => 5, size => 10000} => sub {
    my ($collection, $err) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 drop

  $collection->drop;

Drop collection. You can also append a callback to perform operation
non-blocking.

  $collection->drop(sub {
    my ($collection, $err) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 drop_index

  $collection->drop_index('foo');

Drop index. You can also append a callback to perform operation non-blocking.

  $collection->drop_index(foo => sub {
    my ($collection, $err) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 ensure_index

  $collection->ensure_index(bson_doc(foo => 1, bar => -1));
  $collection->ensure_index({foo => 1});
  $collection->ensure_index({foo => 1}, {unique => bson_true});

Make sure an index exists, the order of keys matters for compound indexes,
additional options will be passed along to the server verbatim. You can also
append a callback to perform operation non-blocking.

  $collection->ensure_index(({foo => 1}, {unique => bson_true}) => sub {
    my ($collection, $err) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 find

  my $cursor = $collection->find;
  my $cursor = $collection->find({foo => 'bar'});
  my $cursor = $collection->find({foo => 'bar'}, {foo => 1});

Build L<Mango::Cursor> object for query.

  # Exclude "_id" field from results
  my $docs = $collection->find({foo => 'bar'}, {_id => 0})->all;

=head2 find_and_modify

  my $doc = $collection->find_and_modify(
    {query => {foo => 'bar'}, update => {'$set' => {foo => 'baz'}}});

Update document atomically. You can also append a callback to perform
operation non-blocking.

  my $opts = {query => {foo => 'bar'}, update => {'$set' => {foo => 'baz'}}};
  $collection->find_and_modify($opts => sub {
    my ($collection, $err, $doc) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 find_one

  my $doc = $collection->find_one({foo => 'bar'});
  my $doc = $collection->find_one({foo => 'bar'}, {foo => 1});
  my $doc = $collection->find_one($oid, {foo => 1});

Find one document. You can also append a callback to perform operation
non-blocking.

  $collection->find_one({foo => 'bar'} => sub {
    my ($collection, $err, $doc) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 full_name

  my $name = $collection->full_name;

Full name of this collection.

=head2 index_information

  my $info = $collection->index_information;

Get index information for collection. You can also append a callback to
perform operation non-blocking.

  $collection->index_information(sub {
    my ($collection, $err, $info) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 insert

  my $oid  = $collection->insert({foo => 'bar'});
  my $oids = $collection->insert([{foo => 'bar'}, {baz => 'yada'}]);

Insert one or more documents into collection. You can also append a callback
to perform operation non-blocking.

  $collection->insert({foo => 'bar'} => sub {
    my ($collection, $err, $oid) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 map_reduce

  my $collection = $collection->map_reduce($map, $reduce, {out => 'foo'});
  my $docs = $collection->map_reduce($map, $reduce, {out => {inline => 1}});
  my $docs = $collection->map_reduce(
    bson_code($map), bson_code($reduce), {out => {inline => 1}});

Perform map/reduce operation on collection, additional options will be passed
along to the server verbatim. You can also append a callback to perform
operation non-blocking.

  $collection->map_reduce(($map, $reduce, {out => {inline => 1}}) => sub {
      my ($collection, $err, $docs) = @_;
      ...
    }
  );
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 options

  my $doc = $collection->options;

Get options for collection. You can also append a callback to perform
operation non-blocking.

  $collection->options(sub {
    my ($collection, $err, $doc) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 remove

  my $doc = $collection->remove;
  my $doc = $collection->remove($oid);
  my $doc = $collection->remove({foo => 'bar'});
  my $doc = $collection->remove({foo => 'bar'}, {single => 1});

Remove documents from collection. You can also append a callback to perform
operation non-blocking.

  $collection->remove(({foo => 'bar'}, {single => 1}) => sub {
    my ($collection, $err, $doc) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

These options are currently available:

=over 2

=item single

  single => 1

Remove only one document.

=back

=head2 save

  my $oid = $collection->save({foo => 'bar'});

Save document to collection. You can also append a callback to perform
operation non-blocking.

  $collection->save({foo => 'bar'} => sub {
    my ($collection, $err, $oid) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 stats

  my $stats = $collection->stats;

Get collection statistics. You can also append a callback to perform operation
non-blocking.

  $collection->stats(sub {
    my ($collection, $err, $stats) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 update

  my $doc = $collection->update({foo => 'bar'}, {foo => 'baz'});
  my $doc = $collection->update({foo => 'bar'}, {foo => 'baz'}, {multi => 1});

Update document in collection. You can also append a callback to perform
operation non-blocking.

  $collection->update(({foo => 'bar'}, {foo => 'baz'}, {multi => 1}) => sub {
    my ($collection, $err, $doc) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

These options are currently available:

=over 2

=item multi

  multi => 1

Update more than one document.

=item upsert

  upsert => 1

Insert document if none could be updated.

=back

=head1 SEE ALSO

L<Mango>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
