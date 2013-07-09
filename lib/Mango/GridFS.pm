package Mango::GridFS;
use Mojo::Base -base;

use Mango::GridFS::Reader;
use Mango::GridFS::Writer;

has chunks => sub { $_[0]->db->collection($_[0]->prefix . '.chunks') };
has 'db';
has files => sub { $_[0]->db->collection($_[0]->prefix . '.files') };
has prefix => 'fs';

sub delete {
  my ($self, $oid) = @_;
  $self->files->remove({_id      => $oid});
  $self->files->remove({files_id => $oid});
}

sub list { shift->files->find->distinct('filename') }

sub reader { Mango::GridFS::Reader->new(gridfs => shift) }
sub writer { Mango::GridFS::Writer->new(gridfs => shift) }

1;

=encoding utf8

=head1 NAME

Mango::GridFS - GridFS

=head1 SYNOPSIS

  use Mango::GridFS;

  my $gridfs = Mango::GridFS->new(db => $db);

=head1 DESCRIPTION

L<Mango::GridFS> is an interface for MongoDB GridFS access.

=head1 ATTRIBUTES

L<Mango::GridFS> implements the following attributes.

=head2 chunks

  my $chunks = $gridfs->chunks;
  $gridfs    = $gridfs->chunks(Mango::Collection->new);

L<Mango::Collection> object for C<chunks> collection, defaults to one based on
C<prefix>.

=head2 db

  my $db  = $gridfs->db;
  $gridfs = $gridfs->db(Mango::Database->new);

L<Mango::Database> object GridFS belongs to.

=head2 files

  my $files = $gridfs->files;
  $gridfs   = $gridfs->files(Mango::Collection->new);

L<Mango::Collection> object for C<files> collection, defaults to one based on
C<prefix>.

=head2 prefix

  my $db  = $gridfs->prefix;
  $gridfs = $gridfs->prefix('foo');

Prefix for GridFS collections, defaults to C<fs>.

=head1 METHODS

L<Mango::GridFS> inherits all methods from L<Mojo::Base> and implements the
following new ones.

=head2 delete

  $gridfs->delete(bson_oid '1a2b3c4e5f60718293a4b5c6');

Delete file.

=head2 list

  my $names = $gridfs->list;

List files.

=head2 reader

  my $reader = $gridfs->reader;

Get L<Mango::GridFS::Reader> object.

=head2 writer

  my $writer = $gridfs->writer;

Get L<Mango::GridFS::Writer> object.

=head1 SEE ALSO

L<Mango>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
