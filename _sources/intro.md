# Saving the planet with data: deriving insights from the Climate Trace project.

Our human activities are responsible for most of the greenhouse gases (GHG) 
released in the atmosphere. These gas stay in the atmosphere for long periods  of time and they drive profound changes in the physical systems and the ecosystems around us. Where do they come from? What can we do about it?

You cannot control what you cannot measure. Identifying the source of these emissions is a key step for eventually reducing them. The [Climate TRACE](https://climatetrace.org/) project is a consortium of research labs, non-profit organizations and companies that aims at providing a comprehensive, global understanding of all the emissions around the planet. It makes high level datasets available for anyone to browse and share.

This handbook is a series of tutorials aimed at data scientists and technical people who may be wondering how to explore the dataset offered by Climate TRACE. I hope that this series of notebooks inspires scientists and engineers to understand better the source of emissions around them, and to give them the curiosity to engage and improve this information.

Even if we are going to analyze hundreds of millions of sources of GHG gases on a planetary scale, any reasonably modern laptop should be enough to run these. Along the way, I will use and point out the modern data science techniques that makes this analysis so fast and streamlined. 
The technical sections that also explain the how the code is running will be clearly marked with a `Technical` label. You can skip them if you are just interested in the results.

After reading this handbook, you should be able to do the following:
- access and manipulate the Climate TRACE dataset with Python and Jupyter
- derive basic insights about emissions from this dataset: which countries emit the most, the least, which sorts of gas, etc.
- understand which sectors and countries present the most uncertainty
- compare and combine multiple measurements of the same emission sector into a more precise estimate, using Bayesian techniques

```{warning}
This handbook is still a draft. Numbers are still being checked with experts.
```

```{warning}
This work not affiliated with the Climate TRACE consortium. The Climate TRACE consortium does not sponsor or endorse any content in this handbook.
Any analysis, error, conclusion contained in these pages should not be attributed to the Climate TRACE project but to the author.
```


```{tableofcontents}
```
