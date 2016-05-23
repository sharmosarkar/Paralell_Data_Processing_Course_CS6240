gd <- read.table("final_output.txt")
gd
gd <- read.table("final_output.txt",sep=",")
x <- V[2]
gd <- read.table("final_output.txt",sep=",",header="T")
gd <- read.table("final_output.txt",sep=",",header=T)
gd
gd <- read.table("final_output.txt",sep=",",header=T)
require(ggplot2)
graph <- plot(data=gd,x=V2,y=V3)
gd <- read.table("final_output.txt",sep=",")
require(ggplot2)
graph <- plot(data=gd,x=V2,y=V3)
graph <- qplot(data=gd,x=V2,y=V3)
png("mgraph.png")
plot(graph)
q()
