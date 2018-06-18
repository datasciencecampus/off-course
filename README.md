<img src="images/port.png">

# Analysing UK port and shipping operations using big data


This repo provides the material to support the work undertaken by the Data Science Campus to explore the operation, use and relationships between ports in the UK at a macro level and the behaviour and operational characteristics of ships at a micro level. Specifically we explore:
* ship travelling behaviours
* traffic at ports and related factors
* port capacity utilisation
* national and international port relationships
* inbound ship delays

The [Automatic Identification System (AIS)](<https://en.wikipedia.org/wiki/Automatic_identification_system>) and [Consolidated European Reporting System (CERS)](<http://www.fisg.org.uk/MCA/upload/MSN1817%20-%20Consolidated%20European%20Reporting%20System%20%28CERS%29.pdf>) data sets, provided by Maritime and Coastguard Agency (MCA), were used for this study.

### Automatic Identification System (AIS) data set
The AIS data records the position, speed, heading, bearing, rate of turn and other important information for each ship, at frequent time intervals throughout its voyage. The messages are received in raw encoded format using the [NMEA](<https://en.wikipedia.org/wiki/NMEA_0183>) standard. Decoding functions based on the description [here](<http://www.bosunsmate.org/ais/>), the [AIVDM/AIVDO](<http://www.catb.org/gpsd/AIVDM.html#_types_1_2_and_3_position_report_class_a>) protocol decoding and the [Navigation Center of Excellence](<https://www.navcen.uscg.gov/?pageName=AISmain>) message explanation were developed in Scala.

### Consolidated European Reporting System (CERS) data set
The CERS data set comes from an information management system maintained by MCA, that is intended to capture all UK ship arrival and departure notifications, dangerous and polluting goods notifications, and notifications of port waste infringements and bulk carrier infringements. This information then is forwarded to the EU SafeSeaNet system in accordance with the EU Vessel Traffic Monitoring and Information System Directive (2002/59/EC).

### How to use this repo

The study is implemented through a series of files that can be found in the `./code` folder. Make sure you read the `./code/config_explanation.md` first on how you should set up your config file, collect required data sets and name all your files. Then proceed as follow:

* If your AIS messages are in a raw format then use the decoders in the `./code/1_AIS_decoder` folder to extract them in csv form. You can also find instructions to filter and process the decoded data in `AIS_decoder.md`.

* If you have decoded AIS messages then jump to the `./code/2_preprocessing` folder where the AIS and CERS data sets are being formatted and the Rate of Turn (RoT) of a moving ship is calculated. 

* The travelling behaviour of ships approaching port is explored in `./code/3_segmentation` folder. K-means unsupervised machine learning algorithm is used and six segments representing specific patterns are derived and explained.

* In `./code/4_feature_engineering`, we bring in new features for the delays modelling that will follow.

* Then we perform supervised machine learning using random forests, adaBoost and XGBoost to predict the probability of a ship being delayed in `./code/5_modelling`. We also uncover insights by exploring the relationship between the delay rate and each feature on a univariate basis.

* Finally we provide methods for different ports and voyage insights in `./code/5_modelling` folder. We look into port traffic, amount of delayed trips, movements between ports and movement of hazardous materials. Finally, we present a network analysis between UK and major international ports.

The complete report supporting this repo can be found [here](<https://datasciencecampus.ons.gov.uk/analysing-port-and-shipping-operations-using-big-data>).
