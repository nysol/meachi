require "rubygems"
require "mkmf"

CONFIG['CC'] = "mpicc"

$CPPFLAGS += " -Wall"
$LOCAL_LIBS += " -lstdc++ "

create_makefile("meachi")

