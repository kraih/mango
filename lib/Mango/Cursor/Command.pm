package Mango::Cursor::Command;
use Mojo::Base 'Mango::Cursor';

1;

=encoding utf8

=head1 NAME

Mango::Cursor::Command - MongoDB command cursor

=head1 SYNOPSIS

  use Mango::Cursor::Command;

  my $cursor = Mango::Cursor::Command->new(collection => $collection);
  my $docs   = $cursor->all;

=head1 DESCRIPTION

L<Mango::Cursor::Command> is a container for MongoDB command cursors used by
L<Mango::Collection>.

=head1 ATTRIBUTES

L<Mango::Cursor::Command> inherits all attributes from L<Mango::Cursor>.

=head1 METHODS

L<Mango::Cursor::Command> inherits all methods from L<Mango::Cursor>.

=head1 SEE ALSO

L<Mango>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
