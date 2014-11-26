#!/usr/bin/env python

import sys
output_value = 1

for line in sys.stdin:
  line = line.strip()
  columns = line.split(",")
  output_key = columns[0] + ',' + columns[1]

  if len(columns) > 14 and columns[14].isdigit() and int(columns[14]) > 0:
    print '%s\t%s' % (output_key, output_value)