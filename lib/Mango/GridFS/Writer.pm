package Mango::GridFS::Writer;
use Mojo::Base -base;

use Mango::BSON qw(bson_bin bson_doc bson_oid bson_time bson_true);

has chunk_size => 262144;
has [qw(filename gridfs)];
has id => sub {bson_oid};

sub close {
  my $self = shift;

  $self->_chunk;

  my $gridfs = $self->gridfs;
  my $files  = $gridfs->files;
  $files->ensure_index({filename => 1});
  $gridfs->chunks->ensure_index(bson_doc(files_id => 1, n => 1),
    {unique => bson_true});

  my $command = bson_doc
    filemd5 => $self->id,
    root    => $gridfs->prefix;
  my $md5 = $gridfs->db->command($command)->{md5};

  my $doc = {
    _id        => $self->id,
    length     => $self->{len},
    chunkSize  => $self->chunk_size,
    uploadDate => bson_time,
    md5        => $md5
  };
  if (my $name = $self->filename) { $doc->{filename} = $name }
  $files->insert($doc);
}

sub write {
  my ($self, $chunk) = @_;
  $self->{buffer} .= $chunk;
  $self->{len} += length $chunk;
  $self->_chunk while length $self->{buffer} > $self->chunk_size;
}

sub _chunk {
  my $self = shift;

  my $chunk = substr $self->{buffer}, 0, $self->chunk_size, '';
  return unless length $chunk;

  my $n      = $self->{n}++;
  my $chunks = $self->gridfs->chunks;
  $chunks->insert({files_id => $self->id, n => $n, data => bson_bin($chunk)});
}

1;

=encoding utf8

=head1 NAME

Mango::GridFS::Writer - GridFS writer

=head1 SYNOPSIS

  use Mango::GridFS::Writer;

  my $writer = Mango::GridFS::Writer->new(gridfs => $gridfs);

=head1 DESCRIPTION

L<Mango::GridFS::Writer> writes files to GridFS.

=head1 ATTRIBUTES

L<Mango::GridFS::Writer> implements the following attributes.

=head2 chunk_size

  my $size = $writer->chunk_size;
  $writer  = $writer->chunk_size(1024);

Chunk size in bytes, defaults to C<262144>.

=head2 filename

  my $name = $writer->filename;
  $writer  = $writer->filename('foo.txt');

Name of file.

=head2 gridfs

  my $gridfs = $writer->gridfs;
  $writer    = $writer->gridfs(Mango::GridFS->new);

L<Mango::GridFS> object this writer belongs to.

=head2 id

  my $id  = $writer->id;
  $writer = $writer->id(bson_oid '1a2b3c4e5f60718293a4b5c6');

Object id of file, defaults to a newly generated one.

=head1 METHODS

L<Mango::GridFS::Writer> inherits all methods from L<Mojo::Base> and
implements the following new ones.

=head2 close

  $writer->close;

Close file.

=head2 write

  $writer->write('hello world!');

Write chunk.

=head1 SEE ALSO

L<Mango>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
