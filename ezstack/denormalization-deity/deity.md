# Denormalization Deity  
  
This module contains the denormalization deity. The deity takes in queries from EZapp, and turns the queries into their respective rules. Response time information from these queries is stored into Histograms, which keep relevant information about the queries for future use. The response times will decide which rules are the most important rules to be enacted by the denormalizer, and which are not.  
  
## Rule Calculation  
  
The response times that are stored in histograms are calculated into scores that determine how inefficient the queries are. The way the scores are calculated, is that the **_mean absolute deviation around the median_** is calculated on the representative set of response times for each respective query. If you'd like to learn more about the math behind this, you can start by reading [this.](https://en.wikipedia.org/wiki/Average_absolute_deviation#Mean_absolute_deviation_around_the_median)  
  
## Histogram Format  
  
The histograms used to store response time information for unique queries store them in reservoirs. Specifically, the reservoir is a UniformReservoir, which stores a representative sample of 1028 response times. These 1028 values are managed by using Vitter's Algorithm R to keep a statistically representative sample. If you'd like to learn more about the math behind this, [this article by Doctor Jeffrey Vitter](https://www.cs.umd.edu/~samir/498/vitter.pdf) contains the entire methodolgy used for the implemenation of the algorithm with respect to representative samples in reservoir storage.
  
## Configuration  
  
| Configurable  | Usage         | Default |
| ------------- |---------------| --------|
| deity.update.interval.secs | This value is the the interval between executions of the rule implementation system, in seconds. | 3600  |
| deity.clientfactory.uri.address | This value is the address that is used for the EZappClientFactory. | http://localhost:8080 |
| deity.cache.interval.secs | This is the interval between updates of the rule cache, so that when a RuleDoesExist calculation is made, it will be more efficient. | 10 |
| deity.update.query.threshold | This value is the threshold of queries made once every deity.update.interval.secs in order for the rule implementation system to execute. If this value is not met, it is zeroed before the start of the next interval. | 10000 |
 | deity.max.histogram.count | This is the maximum amount of unique histograms that can be stored. When this value is met, the deity will prune the histograms in order to remove unnecessary and unused histograms to make room for future histograms. | 50000 |
| deity.max.rule.capacity | This is the maximum count of rules that can be made by the denormalizer at any given point in time. The deity will not attempt to send additional rules to the denormalizer if this maximum has been met. | 20 |
| deity.datadog.key | This is the personalized key that will allow users to see query throughput data for queries going through the deity. Users will have to generate their own keys| No default value |

