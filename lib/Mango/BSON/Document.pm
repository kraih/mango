package Mango::BSON::Document;
use Mojo::Base 'Tie::Hash';

sub DELETE {
  my ($self, $key) = @_;

  if (exists $self->[0]{$key}) {
    for (0 .. $#{$self->[1]}) {
      splice @{$self->[1]}, $_, 1 and last if $key eq $self->[1][$_];
    }
    return delete $self->[0]{$key};
  }

  return undef;
}

sub EXISTS { exists $_[0][0]{$_[1]} }

sub FETCH {
  my ($self, $key) = @_;
  return $self->[0]{$key};
}

sub FIRSTKEY {
  $_[0][2] = 0;
  &NEXTKEY;
}

sub NEXTKEY {
  return $_[0][1][$_[0][2]++] if $_[0][2] <= $#{$_[0][1]};
  return undef;
}

sub STORE {
  my ($self, $key, $value) = @_;

  unless (exists $self->[0]{$key}) {
    push @{$self->[1]}, $key;
  }
  $self->[0]{$key} = $value;
}

sub TIEHASH {
  my $self = bless [{}, [], 0], shift;
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
