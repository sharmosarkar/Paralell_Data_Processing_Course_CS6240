#data <- read.table("part-r-00000");
#summary(data)a
#install.packages("sqldf")
require(knitr)
require(markdown)
require(sqldf)
require(ggplot2)
knit('AnalysisReport.Rmd', 'Report.md')
markdownToHTML('Report.md', 'MyReport.html',option=c("use_xhml"))
system("pandoc -s MyReport.html -o MyReport.pdf")
