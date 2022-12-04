# Instructions
1. Clone this repo into your chosen root folder.
2. 
```
cd zansari_etl_master\
```
3. Decode the steps here to generate data (For data_gen option) (optional)

```
Z2l0IGNsb25lIC0tcmVjdXJzaXZlIGdpdEBnaXRodWIuY29tOmxvdmFzb2EvVFBDSC1zcWxpdGUuZ2l0CmNkIFRQQ0gtc3FsaXRlCm1ha2U=
```
4. To load data from data.zip into star schema. Run:
```
python3 main.py
```
5. Run tests by running:
```
python3 test.py
```
6. Run reports.ipynb notebook to get solutions to all report questions.

### For data_gen option

7. Load data from generated file to star schema (will replace any old data) by running :

```
python3 main.py data_gen
```

8. Run tests by running:
```
python3 test.py data_gen
```

9. Run reports.ipynb notebook to get solutions to all report questions.

