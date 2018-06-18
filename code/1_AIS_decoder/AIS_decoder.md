## AIS2- Processing the shipping tracking information to extract useful insights

AIS2- Processing the shipping tracking information to extract useful insights
This repo contains a number of files that decode raw AIS messages and extract information in convenient format for further processing as follows:

1. /src/main/scala/Ingestion.scala - reads multiple csv AIS files placed in a folder and assembles the messages that are split in two or more lines into single a single line. The output is written to a single parquet file, i.e. AISraw

2. /src/main/scala/Decoding1, /src/main/scala/Decoding4, /src/main/scala/Decoding5 – read from the AISraw file created earlier and decodes the AIS messages according to the AIS standard. Then it saves the different message types into separate files, each with specific file structure that corresponds to the specific message type

3. /src/main/scala/Extraction - given the top left, the bottom right corners of a rectangular area and the number of the required sub-cells this code reads the decoded messages and calculates indicators for each sub-cell of the grid. The indicators are of the type: ships-per-hour, ships-per-day of the year, ships-per-day of the week for each sub-cell. The results are saved to a file which is then used for creating time progression heat maps about usage of the specified area.

4. /src/main/scala/Filtering, /src/main/scala/GridStatsSPD, /src/main/scala/GridStatsSPDW, /src/main/scala/GridStatsSPH, /src/main/scala/GridStatsSPTD - given the top left, the bottom right corners of a rectangular area and the number of the required sub-cells this code reads the decoded messages and calculates indicators for each sub-cell of the grid. The indicators are of the type: ships-per-hour (SPH), ships-per-day (SPD) of the year, ships-per-day of the week (SPDW) for each sub-cell. The results are saved to a file which is then used for creating time progression heat maps about usage of the specified area.