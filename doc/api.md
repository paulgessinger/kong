API
===

From python, kong has an API that is very similar to the REPL. You can get an instance like

```python
import kong
state = kong.get_instance()

state.ls()
state.create_job(command="sleep 10")
```

``` ..autofunction:: kong.get_instance
```


```eval_rst
State
-----

.. autoclass:: kong.state.State
    :members:

.. autoclass:: kong.config.Config
    :members:


Models
------

.. autoclass:: kong.model.folder.Folder
    :members:

.. autoclass:: kong.model.job.Job
    :members:
```
