# Step 3 - Calculating the time-explicit LCI

Calculating the time-explicit LCI from the timeline is very simple, at least from the user perspective:

```python
tlca.lci()
```

Under the hood, we re-build the technosphere and biosphere matrices, adding new rows and columns to carry the extra temporal information. More on that in the [Theory Section](../theory.md#time-mapping).

Now that the inventory is calculated, we can characterize it in the next step.