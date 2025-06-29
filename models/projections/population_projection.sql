{{ config(
    materialized = 'table',
    indexes=[{'columns': ['county']}, {'columns': ['year']}, {'columns': ['county', 'year'], 'unique': true}]
) }}

SELECT * FROM (VALUES
    ('Mombasa', 2020, 1228079),
    ('Mombasa', 2021, 1256006),
    ('Mombasa', 2022, 1283933),
    ('Mombasa', 2023, 1311860),
    ('Mombasa', 2024, 1339787),
    ('Mombasa', 2025, 1367714),
    ('Mombasa', 2030, 1504530),
    ('Mombasa', 2035, 1635074),
    ('Mombasa', 2040, 1757673),
    ('Mombasa', 2045, 1872463),
    ('Kwale', 2020, 879076),
    ('Kwale', 2021, 900872),
    ('Kwale', 2022, 922668),
    ('Kwale', 2023, 944464),
    ('Kwale', 2024, 966260),
    ('Kwale', 2025, 988056),
    ('Kwale', 2030, 1112022),
    ('Kwale', 2035, 1246928),
    ('Kwale', 2040, 1385948),
    ('Kwale', 2045, 1524725),
    ('Kilifi', 2020, 1488572),
    ('Kilifi', 2021, 1518160),
    ('Kilifi', 2022, 1547747),
    ('Kilifi', 2023, 1577335),
    ('Kilifi', 2024, 1606922),
    ('Kilifi', 2025, 1636510),
    ('Kilifi', 2030, 1785800),
    ('Kilifi', 2035, 1933624),
    ('Kilifi', 2040, 2077150),
    ('Kilifi', 2045, 2214483),
    ('Tana River', 2020, 325873),
    ('Tana River', 2021, 334765),
    ('Tana River', 2022, 343657),
    ('Tana River', 2023, 352549),
    ('Tana River', 2024, 361440),
    ('Tana River', 2025, 370332),
    ('Tana River', 2030, 420374),
    ('Tana River', 2035, 475138),
    ('Tana River', 2040, 532615),
    ('Tana River', 2045, 590644),
    ('Lamu', 2020, 154774),
    ('Lamu', 2021, 158960),
    ('Lamu', 2022, 163146),
    ('Lamu', 2023, 167332),
    ('Lamu', 2024, 171519),
    ('Lamu', 2025, 175705),
    ('Lamu', 2030, 198455),
    ('Lamu', 2035, 222752),
    ('Lamu', 2040, 247918),
    ('Lamu', 2045, 273139),
    ('Taita Taveta', 2020, 350614),
    ('Taita Taveta', 2021, 355073),
    ('Taita Taveta', 2022, 359531),
    ('Taita Taveta', 2023, 363990),
    ('Taita Taveta', 2024, 368448),
    ('Taita Taveta', 2025, 372907),
    ('Taita Taveta', 2030, 394539),
    ('Taita Taveta', 2035, 416076),
    ('Taita Taveta', 2040, 437287),
    ('Taita Taveta', 2045, 457173),
    ('Garissa', 2020, 861201),
    ('Garissa', 2021, 883144),
    ('Garissa', 2022, 905087),
    ('Garissa', 2023, 927031),
    ('Garissa', 2024, 948974),
    ('Garissa', 2025, 970917),
    ('Garissa', 2030, 1075926),
    ('Garissa', 2035, 1175079),
    ('Garissa', 2040, 1267672),
    ('Garissa', 2045, 1353654),
    ('Wajir', 2020, 803882),
    ('Wajir', 2021, 826133),
    ('Wajir', 2022, 848385),
    ('Wajir', 2023, 870636),
    ('Wajir', 2024, 892887),
    ('Wajir', 2025, 915139),
    ('Wajir', 2030, 1037827),
    ('Wajir', 2035, 1164812),
    ('Wajir', 2040, 1289502),
    ('Wajir', 2045, 1407640),
    ('Mandera', 2020, 887280),
    ('Mandera', 2021, 911265),
    ('Mandera', 2022, 935251),
    ('Mandera', 2023, 959236),
    ('Mandera', 2024, 983222),
    ('Mandera', 2025, 1007207),
    ('Mandera', 2030, 1139779),
    ('Mandera', 2035, 1276744),
    ('Mandera', 2040, 1411270),
    ('Mandera', 2045, 1541077),
    ('Marsabit', 2020, 479579),
    ('Marsabit', 2021, 491483),
    ('Marsabit', 2022, 503388),
    ('Marsabit', 2023, 515292),
    ('Marsabit', 2024, 527197),
    ('Marsabit', 2025, 539101),
    ('Marsabit', 2030, 604075),
    ('Marsabit', 2035, 670479),
    ('Marsabit', 2040, 734375),
    ('Marsabit', 2045, 793445),
    ('Isiolo', 2020, 294104),
    ('Isiolo', 2021, 301382),
    ('Isiolo', 2022, 308659),
    ('Isiolo', 2023, 315937),
    ('Isiolo', 2024, 323215),
    ('Isiolo', 2025, 330492),
    ('Isiolo', 2030, 368938),
    ('Isiolo', 2035, 408630),
    ('Isiolo', 2040, 448487),
    ('Isiolo', 2045, 487378),
    ('Meru', 2020, 1565421),
    ('Meru', 2021, 1585608),
    ('Meru', 2022, 1605795),
    ('Meru', 2023, 1625982),
    ('Meru', 2024, 1646169),
    ('Meru', 2025, 1666357),
    ('Meru', 2030, 1765151),
    ('Meru', 2035, 1859343),
    ('Meru', 2040, 1946218),
    ('Meru', 2045, 2024342),
    ('Tharaka-Nithi', 2020, 403102),
    ('Tharaka-Nithi', 2021, 407529),
    ('Tharaka-Nithi', 2022, 411956),
    ('Tharaka-Nithi', 2023, 416383),
    ('Tharaka-Nithi', 2024, 420811),
    ('Tharaka-Nithi', 2025, 425238),
    ('Tharaka-Nithi', 2030, 445537),
    ('Tharaka-Nithi', 2035, 464028),
    ('Tharaka-Nithi', 2040, 480668),
    ('Tharaka-Nithi', 2045, 495767),
    ('Embu', 2020, 628527),
    ('Embu', 2021, 635160),
    ('Embu', 2022, 641792),
    ('Embu', 2023, 648425),
    ('Embu', 2024, 655057),
    ('Embu', 2025, 661690),
    ('Embu', 2030, 692132),
    ('Embu', 2035, 719716),
    ('Embu', 2040, 744452),
    ('Embu', 2045, 766431),
    ('Kitui', 2020, 1186046),
    ('Kitui', 2021, 1200627),
    ('Kitui', 2022, 1215209),
    ('Kitui', 2023, 1229790),
    ('Kitui', 2024, 1244372),
    ('Kitui', 2025, 1258953),
    ('Kitui', 2030, 1327464),
    ('Kitui', 2035, 1390005),
    ('Kitui', 2040, 1446285),
    ('Kitui', 2045, 1494475),
    ('Machakos', 2020, 1441719),
    ('Machakos', 2021, 1457065),
    ('Machakos', 2022, 1472411),
    ('Machakos', 2023, 1487758),
    ('Machakos', 2024, 1503104),
    ('Machakos', 2025, 1518450),
    ('Machakos', 2030, 1584422),
    ('Machakos', 2035, 1641690),
    ('Machakos', 2040, 1690853),
    ('Machakos', 2045, 1732802),
    ('Makueni', 2020, 1007527),
    ('Makueni', 2021, 1019118),
    ('Makueni', 2022, 1030709),
    ('Makueni', 2023, 1042300),
    ('Makueni', 2024, 1053891),
    ('Makueni', 2025, 1065482),
    ('Makueni', 2030, 1121214),
    ('Makueni', 2035, 1174901),
    ('Makueni', 2040, 1225448),
    ('Makueni', 2045, 1271110),
    ('Nyandarua', 2020, 657159),
    ('Nyandarua', 2021, 669950),
    ('Nyandarua', 2022, 682740),
    ('Nyandarua', 2023, 695531),
    ('Nyandarua', 2024, 708321),
    ('Nyandarua', 2025, 721112),
    ('Nyandarua', 2030, 783354),
    ('Nyandarua', 2035, 842273),
    ('Nyandarua', 2040, 897238),
    ('Nyandarua', 2045, 948517),
    ('Nyeri', 2020, 809599),
    ('Nyeri', 2021, 818202),
    ('Nyeri', 2022, 826805),
    ('Nyeri', 2023, 835408),
    ('Nyeri', 2024, 844011),
    ('Nyeri', 2025, 852614),
    ('Nyeri', 2030, 894578),
    ('Nyeri', 2035, 933388),
    ('Nyeri', 2040, 969207),
    ('Nyeri', 2045, 1002143),
    ('Kirinyaga', 2020, 637139),
    ('Kirinyaga', 2021, 642463),
    ('Kirinyaga', 2022, 647788),
    ('Kirinyaga', 2023, 653112),
    ('Kirinyaga', 2024, 658436),
    ('Kirinyaga', 2025, 663760),
    ('Kirinyaga', 2030, 690207),
    ('Kirinyaga', 2035, 715457),
    ('Kirinyaga', 2040, 738800),
    ('Kirinyaga', 2045, 759772),
    ('Muranga', 2020, 1076540),
    ('Muranga', 2021, 1088456),
    ('Muranga', 2022, 1100372),
    ('Muranga', 2023, 1112288),
    ('Muranga', 2024, 1124204),
    ('Muranga', 2025, 1136120),
    ('Muranga', 2030, 1193960),
    ('Muranga', 2035, 1248813),
    ('Muranga', 2040, 1299390),
    ('Muranga', 2045, 1345265),
    ('Kiambu', 2020, 2500990),
    ('Kiambu', 2021, 2551620),
    ('Kiambu', 2022, 2602250),
    ('Kiambu', 2023, 2652880),
    ('Kiambu', 2024, 2703510),
    ('Kiambu', 2025, 2754139),
    ('Kiambu', 2030, 3006176),
    ('Kiambu', 2035, 3250669),
    ('Kiambu', 2040, 3487491),
    ('Kiambu', 2045, 3717358),
    ('Turkana', 2020, 946464),
    ('Turkana', 2021, 971900),
    ('Turkana', 2022, 997337),
    ('Turkana', 2023, 1022773),
    ('Turkana', 2024, 1048210),
    ('Turkana', 2025, 1073646),
    ('Turkana', 2030, 1216202),
    ('Turkana', 2035, 1364902),
    ('Turkana', 2040, 1511677),
    ('Turkana', 2045, 1652023),
    ('West Pokot', 2020, 631122),
    ('West Pokot', 2021, 646190),
    ('West Pokot', 2022, 661258),
    ('West Pokot', 2023, 676326),
    ('West Pokot', 2024, 691394),
    ('West Pokot', 2025, 706462),
    ('West Pokot', 2030, 791958),
    ('West Pokot', 2035, 882553),
    ('West Pokot', 2040, 973370),
    ('West Pokot', 2045, 1061255),
    ('Samburu', 2020, 320308),
    ('Samburu', 2021, 329638),
    ('Samburu', 2022, 338968),
    ('Samburu', 2023, 348298),
    ('Samburu', 2024, 357628),
    ('Samburu', 2025, 366958),
    ('Samburu', 2030, 419849),
    ('Samburu', 2035, 476273),
    ('Samburu', 2040, 533272),
    ('Samburu', 2045, 588833),
    ('Trans-Nzoia', 2020, 1010265),
    ('Trans-Nzoia', 2021, 1029856),
    ('Trans-Nzoia', 2022, 1049448),
    ('Trans-Nzoia', 2023, 1069039),
    ('Trans-Nzoia', 2024, 1088630),
    ('Trans-Nzoia', 2025, 1108221),
    ('Trans-Nzoia', 2030, 1198602),
    ('Trans-Nzoia', 2035, 1283065),
    ('Trans-Nzoia', 2040, 1362351),
    ('Trans-Nzoia', 2045, 1435567),
    ('Uasin-Gishu', 2020, 1183030),
    ('Uasin-Gishu', 2021, 1207797),
    ('Uasin-Gishu', 2022, 1232564),
    ('Uasin-Gishu', 2023, 1257330),
    ('Uasin-Gishu', 2024, 1282097),
    ('Uasin-Gishu', 2025, 1306864),
    ('Uasin-Gishu', 2030, 1428167),
    ('Uasin-Gishu', 2035, 1545995),
    ('Uasin-Gishu', 2040, 1659270),
    ('Uasin-Gishu', 2045, 1765591),
    ('Elgeyo-Marakwet', 2020, 474419),
    ('Elgeyo-Marakwet', 2021, 481359),
    ('Elgeyo-Marakwet', 2022, 488299),
    ('Elgeyo-Marakwet', 2023, 495239),
    ('Elgeyo-Marakwet', 2024, 502179),
    ('Elgeyo-Marakwet', 2025, 509119),
    ('Elgeyo-Marakwet', 2030, 541933),
    ('Elgeyo-Marakwet', 2035, 572433),
    ('Elgeyo-Marakwet', 2040, 599559),
    ('Elgeyo-Marakwet', 2045, 622588),
    ('Nandi', 2020, 905629),
    ('Nandi', 2021, 920906),
    ('Nandi', 2022, 936183),
    ('Nandi', 2023, 951460),
    ('Nandi', 2024, 966737),
    ('Nandi', 2025, 982014),
    ('Nandi', 2030, 1054270),
    ('Nandi', 2035, 1121556),
    ('Nandi', 2040, 1183280),
    ('Nandi', 2045, 1238770),
    ('Baringo', 2020, 686717),
    ('Baringo', 2021, 702256),
    ('Baringo', 2022, 717794),
    ('Baringo', 2023, 733333),
    ('Baringo', 2024, 748872),
    ('Baringo', 2025, 764411),
    ('Baringo', 2030, 840367),
    ('Baringo', 2035, 911115),
    ('Baringo', 2040, 974481),
    ('Baringo', 2045, 1029990),
    ('Laikipia', 2020, 528509),
    ('Laikipia', 2021, 539414),
    ('Laikipia', 2022, 550318),
    ('Laikipia', 2023, 561223),
    ('Laikipia', 2024, 572128),
    ('Laikipia', 2025, 583033),
    ('Laikipia', 2030, 639451),
    ('Laikipia', 2035, 695738),
    ('Laikipia', 2040, 750258),
    ('Laikipia', 2045, 801993),
    ('Nakuru', 2020, 2201828),
    ('Nakuru', 2021, 2250502),
    ('Nakuru', 2022, 2299175),
    ('Nakuru', 2023, 2347849),
    ('Nakuru', 2024, 2396522),
    ('Nakuru', 2025, 2445196),
    ('Nakuru', 2030, 2689907),
    ('Nakuru', 2035, 2928650),
    ('Nakuru', 2040, 3156425),
    ('Nakuru', 2045, 3371874),
    ('Narok', 2020, 1177718),
    ('Narok', 2021, 1213213),
    ('Narok', 2022, 1248708),
    ('Narok', 2023, 1284204),
    ('Narok', 2024, 1319699),
    ('Narok', 2025, 1355194),
    ('Narok', 2030, 1546071),
    ('Narok', 2035, 1739888),
    ('Narok', 2040, 1928875),
    ('Narok', 2045, 2109584),
    ('Kajiado', 2020, 1178759),
    ('Kajiado', 2021, 1208593),
    ('Kajiado', 2022, 1238427),
    ('Kajiado', 2023, 1268261),
    ('Kajiado', 2024, 1298095),
    ('Kajiado', 2025, 1327929),
    ('Kajiado', 2030, 1475089),
    ('Kajiado', 2035, 1619210),
    ('Kajiado', 2040, 1759061),
    ('Kajiado', 2045, 1893834),
    ('Kericho', 2020, 917217),
    ('Kericho', 2021, 929777),
    ('Kericho', 2022, 942337),
    ('Kericho', 2023, 954896),
    ('Kericho', 2024, 967456),
    ('Kericho', 2025, 980016),
    ('Kericho', 2030, 1037078),
    ('Kericho', 2035, 1089354),
    ('Kericho', 2040, 1136611),
    ('Kericho', 2045, 1178485),
    ('Bomet', 2020, 901539),
    ('Bomet', 2021, 914280),
    ('Bomet', 2022, 927020),
    ('Bomet', 2023, 939761),
    ('Bomet', 2024, 952502),
    ('Bomet', 2025, 965243),
    ('Bomet', 2030, 1021371),
    ('Bomet', 2035, 1071948),
    ('Bomet', 2040, 1116874),
    ('Bomet', 2045, 1155747),
    ('Kakamega', 2020, 1897240),
    ('Kakamega', 2021, 1932305),
    ('Kakamega', 2022, 1967370),
    ('Kakamega', 2023, 2002435),
    ('Kakamega', 2024, 2037500),
    ('Kakamega', 2025, 2072565),
    ('Kakamega', 2030, 2237189),
    ('Kakamega', 2035, 2389148),
    ('Kakamega', 2040, 2528257),
    ('Kakamega', 2045, 2654106),
    ('Vihiga', 2020, 609926),
    ('Vihiga', 2021, 615206),
    ('Vihiga', 2022, 620485),
    ('Vihiga', 2023, 625765),
    ('Vihiga', 2024, 631045),
    ('Vihiga', 2025, 636325),
    ('Vihiga', 2030, 660333),
    ('Vihiga', 2035, 681333),
    ('Vihiga', 2040, 700034),
    ('Vihiga', 2045, 716355),
    ('Bungoma', 2020, 1700411),
    ('Bungoma', 2021, 1729265),
    ('Bungoma', 2022, 1758119),
    ('Bungoma', 2023, 1786973),
    ('Bungoma', 2024, 1815827),
    ('Bungoma', 2025, 1844681),
    ('Bungoma', 2030, 1969526),
    ('Bungoma', 2035, 2080052),
    ('Bungoma', 2040, 2178467),
    ('Bungoma', 2045, 2264762),
    ('Busia', 2020, 913595),
    ('Busia', 2021, 931984),
    ('Busia', 2022, 950374),
    ('Busia', 2023, 968763),
    ('Busia', 2024, 987152),
    ('Busia', 2025, 1005542),
    ('Busia', 2030, 1095354),
    ('Busia', 2035, 1180523),
    ('Busia', 2040, 1259694),
    ('Busia', 2045, 1331867),
    ('Siaya', 2020, 1002932),
    ('Siaya', 2021, 1021774),
    ('Siaya', 2022, 1040616),
    ('Siaya', 2023, 1059458),
    ('Siaya', 2024, 1078299),
    ('Siaya', 2025, 1097141),
    ('Siaya', 2030, 1195671),
    ('Siaya', 2035, 1294119),
    ('Siaya', 2040, 1388535),
    ('Siaya', 2045, 1477983),
    ('Kisumu', 2020, 1186160),
    ('Kisumu', 2021, 1206931),
    ('Kisumu', 2022, 1227702),
    ('Kisumu', 2023, 1248474),
    ('Kisumu', 2024, 1269245),
    ('Kisumu', 2025, 1290016),
    ('Kisumu', 2030, 1389489),
    ('Kisumu', 2035, 1484366),
    ('Kisumu', 2040, 1574178),
    ('Kisumu', 2045, 1658052),
    ('Homa-Bay', 2020, 1161873),
    ('Homa-Bay', 2021, 1185135),
    ('Homa-Bay', 2022, 1208397),
    ('Homa-Bay', 2023, 1231659),
    ('Homa-Bay', 2024, 1254921),
    ('Homa-Bay', 2025, 1278183),
    ('Homa-Bay', 2030, 1401509),
    ('Homa-Bay', 2035, 1525655),
    ('Homa-Bay', 2040, 1644756),
    ('Homa-Bay', 2045, 1756134),
    ('Migori', 2020, 1147197),
    ('Migori', 2021, 1176159),
    ('Migori', 2022, 1205120),
    ('Migori', 2023, 1234082),
    ('Migori', 2024, 1263044),
    ('Migori', 2025, 1292006),
    ('Migori', 2030, 1444465),
    ('Migori', 2035, 1599862),
    ('Migori', 2040, 1753828),
    ('Migori', 2045, 1903551),
    ('Kisii', 2020, 1306711),
    ('Kisii', 2021, 1319443),
    ('Kisii', 2022, 1332175),
    ('Kisii', 2023, 1344907),
    ('Kisii', 2024, 1357639),
    ('Kisii', 2025, 1370372),
    ('Kisii', 2030, 1423487),
    ('Kisii', 2035, 1467698),
    ('Kisii', 2040, 1504367),
    ('Kisii', 2045, 1535009),
    ('Nyamira', 2020, 645541),
    ('Nyamira', 2021, 649528),
    ('Nyamira', 2022, 653515),
    ('Nyamira', 2023, 657502),
    ('Nyamira', 2024, 661490),
    ('Nyamira', 2025, 665477),
    ('Nyamira', 2030, 682625),
    ('Nyamira', 2035, 697815),
    ('Nyamira', 2040, 711050),
    ('Nyamira', 2045, 722141),
    ('Nairobi', 2020, 4515607),
    ('Nairobi', 2021, 4593757),
    ('Nairobi', 2022, 4671906),
    ('Nairobi', 2023, 4750056),
    ('Nairobi', 2024, 4828205),
    ('Nairobi', 2025, 4906355),
    ('Nairobi', 2030, 5264721),
    ('Nairobi', 2035, 5595922),
    ('Nairobi', 2040, 5902303),
    ('Nairobi', 2045, 6180029)
) AS t(county, year, population)