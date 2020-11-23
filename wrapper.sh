#!/bin/sh
ltrace  -tt -T --output data/trace.$OMPI_COMM_WORLD_RANK $*
