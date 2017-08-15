# intake
Scratchpad for working out some data access / data catalog ideas

### Catalog Description

``` yaml
sales_data:
  description: Individual product sales training
  driver: postgres
  driver_args:
    uri: postgresql://username:passw0rd@dbserver1:5432/sales
  parameters:
    start:
      description: Earliest transaction timestamp
      type: datetime
      default: "2017-01-01 00:00:00"
      min: "2015-01-01 00:00:00"
    end:
      description: Latest transaction timestamp
      type: datetime
      default: "2017-02-01 00:00:00"
      min: "2015-01-01 00:00:00"
  load_args:
    query: "SELECT * FROM sales WHERE timestamp > :start AND timestamp < :end"
    
daily_stock_2017:
  driver: hdf5
  driver_args:
    file: /mnt/prices/2017.hdf5
  load_args:
    dataset: 'daily_stock'  
```
