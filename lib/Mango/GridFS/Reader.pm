package Mango::GridFS::Reader;
use Mojo::Base -base;

has 'gridfs';

sub chunk_size   { shift->{meta}{chunkSize} }
sub content_type { shift->{meta}{contentType} }
sub filename     { shift->{meta}{filename} }
sub metadata     { shift->{meta}{metadata} }

sub open {
  my ($self, $oid, $cb) = @_;

  # Non-blocking
  if ($cb) {
    $self->gridfs->files->find_one(
      $oid => sub {
        my ($collection, $err, $doc) = @_;
        $self->{meta} = $doc;
        $self->$cb($err);
      }
    );
  }

  # Blocking
  else { $self->{meta} = $self->gridfs->files->find_one($oid) }

  return $self;
}

sub read {
  my ($self, $cb) = @_;

  $self->{pos} //= 0;

  # EOF
  if ($self->{pos} >= ($self->size // 0)) {
    return undef unless $cb;
    return Mojo::IOLoop->timer(0 => sub { $self->$cb(undef, undef) });
  }

  # Blocking
  my $n = int($self->{pos} / $self->chunk_size);
  my $doc = {files_id => $self->{meta}{_id}, n => $n};
  return $self->_slice($n, $self->gridfs->chunks->find_one($doc)->{data})
    unless $cb;

  # Non-blocking
  $self->gridfs->chunks->find_one(
    $doc => sub {
      my ($collection, $err, $doc) = @_;
      $self->$cb($err, $self->_slice($n, $doc->{data}));
    }
  );
}

sub seek {
  my ($self, $pos) = @_;
  $self->{pos} = $pos;
  return $self;
}

sub slurp {
  my ($self, $cb) = @_;

  # Blocking
  my $data;
  unless ($cb) {
    while (defined(my $chunk = $self->read)) { $data .= $chunk }
    return $data;
  }

  # Non-blocking
  $self->_chunk(\$data, $cb);
}

sub size { shift->{meta}{length} }

sub tell { shift->{pos} // 0 }

sub upload_date { shift->{meta}{uploadDate} }

sub _chunk {
  my ($self, $dataref, $cb) = @_;

  $self->read(
    sub {
      my ($self, $err, $chunk) = @_;
      return $self->$cb($err, $$dataref) if $err || !defined $chunk;
      $$dataref .= $chunk;
      $self->_chunk($dataref, $cb);
    }
  );
}

sub _slice {
  my ($self, $n, $chunk) = @_;
  my $offset = $self->{pos} - ($n * $self->chunk_size);
  $self->{pos} += length $chunk;
  return substr $chunk, $offset;
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

=head2 chunk_size

  my $size = $reader->chunk_size;

Chunk size in bytes.

=head2 content_type

  my $type = $reader->content_type;

Content type of file.

=head2 filename

  my $name = $reader->filename;

Name of file.

=head2 metadata

  my $data = $reader->metadata;

Additional information.

=head2 open

  $reader = $reader->open($oid);

Open file. You can also append a callback to perform operation non-blocking.

  $reader->open($oid => sub {
    my ($reader, $err) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 read

  my $chunk = $reader->read;

Read chunk. You can also append a callback to perform operation non-blocking.

  $reader->read(sub {
    my ($reader, $err, $chunk) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 seek

  $reader = $reader->seek(13);

Change current position.

=head2 size

  my $size = $reader->size;

Size of entire file in bytes.

=head2 slurp

  my $data = $reader->slurp;

Slurp all remaining data from file. You can also append a callback to perform
operation non-blocking.

  $reader->slurp(sub {
    my ($reader, $err, $data) = @_;
    ...
  });
  Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

=head2 tell

  my $pos = $reader->tell;

Current position.

=head2 upload_date

  my $time = $reader->upload_date;

Date file was uploaded.

=head1 SEE ALSO

L<Mango>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
