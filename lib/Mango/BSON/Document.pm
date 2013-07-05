package Mango::BSON::Document;
use Mojo::Base 'Tie::Hash';

sub DELETE {
  my ($self, $key) = @_;

  if (exists $self->[0]{$key}) {
    my $i = $self->[0]{$key};
    $self->[0]{$self->[1][$_]}-- for $i + 1 .. $#{$self->[1]};
    delete $self->[0]{$key};
    splice @{$self->[1]}, $i, 1;
    return (splice(@{$self->[2]}, $i, 1))[0];
  }

  return undef;
}

sub EXISTS { exists $_[0][0]{$_[1]} }

sub FETCH {
  my ($self, $key) = @_;
  return exists $self->[0]{$key} ? $self->[2][$self->[0]{$key}] : undef;
}

sub FIRSTKEY {
  $_[0][3] = 0;
  &NEXTKEY;
}

sub NEXTKEY {
  return $_[0][1][$_[0][3]++] if $_[0][3] <= $#{$_[0][1]};
  return undef;
}

sub STORE {
  my ($self, $key, $value) = @_;

  if (exists $self->[0]{$key}) {
    my $i = $self->[0]{$key};
    $self->[0]{$key} = $i;
    $self->[1][$i]   = $key;
    $self->[2][$i]   = $value;
  }
  else {
    push @{$self->[1]}, $key;
    push @{$self->[2]}, $value;
    $self->[0]{$key} = $#{$self->[1]};
  }
}

sub TIEHASH {
  my $self = bless [{}, [], [], 0], shift;
  $self->STORE(shift, shift) while @_;
  return $self;
}

1;

=encoding utf8

=head1 NAME

Mango::BSON::Document - Document type

=head1 SYNOPSIS

  use Mango::BSON::Document;

  tie my %hash, 'Mango::BSON::Document';

=head1 DESCRIPTION

L<Mango::BSON::Document> is a container for the BSON document type used by
L<Mango::BSON>.

=head1 SEE ALSO

L<Mango>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
