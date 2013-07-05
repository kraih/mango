package Mango::BSON::Code;
use Mojo::Base -base;

has [qw(code scope)];

1;

=encoding utf8

=head1 NAME

Mango::BSON::Code - Code type

=head1 SYNOPSIS

  use Mango::BSON::Code;

  my $code = Mango::BSON::Code->new(code => 'function () {}');

=head1 DESCRIPTION

L<Mango::BSON::Code> is a container for the BSON code type used by
L<Mango::BSON>.

=head1 ATTRIBUTES

L<Mango::BSON::Code> implements the following attributes.

=head2 code

  my $js = $code->code;
  $code  = $code->code('function () {}');

JavaScript code.

=head2 scope

  my $scode = $code->scope;
  $code     = $code->scope({foo => 'bar'});

Scope.

=head1 METHODS

L<Mango::BSON::Code> inherits all methods from L<Mojo::Base>.

=head1 SEE ALSO

L<Mango>, L<Mojolicious::Guides>, L<http://mojolicio.us>.

=cut
