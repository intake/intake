# intake
Scratchpad for working out some data access / data catalog ideas

### Catalog Description

``` yaml
sales_data:
  description: Individual product sales training
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
  driver: postgres
  driver_args:
    uri: postgresql://username:passw0rd@dbserver1:5432/sales
  load_args:
    query: "SELECT * FROM sales WHERE timestamp > :start AND timestamp < :end"
    start: !template_str "{{ start }}"
    end: !template_str "{{ end }}"

daily_stock:
  description: Daily stock prices
  parameters:
    year:
      description: Year
      type: int
      default: 2017
      min: 2011
      max: 2017
  driver: hdf5
  driver_args:
    file: !template_str "/mnt/stock_prices/{{ year }}.hdf5"
  load_args:
    dataset: 'daily'  
```
