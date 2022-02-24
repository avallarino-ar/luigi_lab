# Luigi Pipelines

Luigi is a Python package that helps you build complex pipelines of batch jobs. 
It handles dependency resolution, workflow management, visualization, 
handling failures, command line integration, and much more.
The purpose of Luigi is to address all the plumbing typically 
associated with long-running batch processes

--- 

Getting Started  
`pip install luigi`

`luigid`
> http://localhost:8082 


+ Target
+ Task
    + run()
    + output()
    + requires(): dependencies between Tasks
    
![](https://luigi.readthedocs.io/en/stable/_images/task_breakdown.png)
    

