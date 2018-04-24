# Denormalization Deity  
  
This module contains the denormalization deity. The deity takes in queries from EZapp, and turns the queries into their respective rules. Response time information from these queries is stored into Histograms, which keep relevant information about the queries for future use. The response times will decide which rules are the most important rules to be enacted by the denormalizer, and which are not.  
  
## Rule Calculation  
  
The response times that are stored in histograms are calculated into scores that determine how inefficient the queries are. The way the scores are calculated, is that the **_mean absolute deviation around the median_** is calculated on the representative set of response times for each respective query. If you'd like to learn more about the math behind this, you can start by reading [this.](https://en.wikipedia.org/wiki/Average_absolute_deviation#Mean_absolute_deviation_around_the_median)  
  
## Histogram Format  
  
The histograms used to store response time information for unique queries store them in reservoirs. Specifically, the reservoir is a UniformReservoir, which stores a representative sample of 1028 response times. These 1028 values are managed by using Vitter's Algorithm R to keep a statistically representative sample. If you'd like to learn more about the math behind this, [this article by Doctor Jeffrey Vitter](https://www.cs.umd.edu/~samir/498/vitter.pdf) contains the entire methodolgy used for the implemenation of the algorithm with respect to representative samples in reservoir storage.
