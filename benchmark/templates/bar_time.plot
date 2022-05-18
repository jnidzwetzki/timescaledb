set encoding utf8
set ylabel "Time [sec]" 

set style data histogram
set style histogram cluster gap 1
set style fill pattern border pattern 1

set boxwidth 0.9
set xtics format ""
set grid ytics

set yrange [0:180]
set xlabel "Dataset" offset 0,-2
set xtics font ", 14"

set title "Execution time - COPY operation (1st import)"
#set terminal postscript enhanced font 'Verdana,16'
#set out '| epstopdf --gray --filter --autorotate=All > time_1.pdf; pdfcrop --margins 5 time_1.pdf > /dev/null'
set terminal png size 1024,768 enhanced font "Verdana,16"
set out 'time_1.png'

plot "data.dat" using ($2/1000):xtic(1) index 0:0 title "PostgreSQL" , \
           ''   using ($3/1000) index 0:0 title "PostgreSQL (range partitioned)" , \
           ''   using ($4/1000) index 0:0 title "TimescaleDB (CIM\\_SINGLE)" , \
           ''   using ($5/1000) index 0:0 title "TimescaleDB (CIM\\_MULTI)" 

set title "Execution time - COPY operation (2nd import)"
#set out '| epstopdf --gray --filter --autorotate=All > time_2.pdf; pdfcrop --margins 5 time_2.pdf > /dev/null'
set out 'time_2.png'
plot "data.dat" using ($2/1000):xtic(1) index 1:1 title "PostgreSQL" , \
           ''   using ($3/1000) index 1:1 title "PostgreSQL (range partitioned)" , \
           ''   using ($4/1000) index 1:1 title "TimescaleDB (CIM\\_SINGLE)" , \
           ''   using ($5/1000) index 1:1 title "TimescaleDB (CIM\\_MULTI)" 

