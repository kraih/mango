package Mango::GridFS::Reader;
use Mojo::Base -base;

has 'gridfs';

sub open {
  my ($self, $oid) = @_;
  my $file = $self->gridfs->files->find_one($oid);
  $self->{id}         = $oid;
  $self->{chunk_size} = $file->{chunkSize};
  $self->{len}        = $file->{length};
}

sub read {
  my $self = shift;

  $self->{pos} //= 0;
  return undef if $self->{pos} >= $self->{len};
  my $n = $self->{pos} / $self->{chunk_size};
  my $chunk
    = $self->gridfs->chunks->find_one({files_id => $self->{id}, n => $n});
  my $data = $chunk->{data};
  $self->{pos} += length $data;
  return $data;
}

1;

=encoding utf8

=head1 NAME

Mango::GridFS::Reader - GridFS reader

=head1 SYNOPSIS

  use Mango::GridFS::Reader;

  my $reader = Mango::GridFS::Reader->new(gridfs => $gridfs);

=head1 DESCRIPTION

L<Mango::GridFS::Reader> reads files from GridFS.

=head1 ATTRIBUTES

L<Mango::GridFS::Reader> implements the following attributes.

=head2 gridfs

  my $gridfs = $reader->gridfs;
  $reader    = $reader->gridfs(Mango::GridFS->new);

L<Mango::GridFS> object this reader belongs to.

=head1 METHODS

L<Mango::GridFS::Reader> inherits all methods from L<Mojo::Base> and
implements the following new ones.

=head2 open

  $reader->open(bson_oid '1a2b3c4e5f60718293a4b5c6');

Open file.

=head2 read

  my $chunk = $reader->read;

Read chunk.

=head1 SEE ALSO

L<Mango>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
