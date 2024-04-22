from neo4j import GraphDatabase
import pandas as pd
from scipy import stats
import geopandas as gpd
import dotenv
import numpy as np
import os
import re


def ReadData(csvPath: str) -> pd.DataFrame:
    data = pd.read_csv(csvPath, header=None,
                       delimiter=";", low_memory=False,
                       encoding="UTF-8").rename(columns={0: "ifcid",
                                                        1: "type",
                                                        2: "date",
                                                        3: "value"})
    data = data.drop(4, axis="columns")
    data["value"] = data["value"].str.replace(",", ".").astype(float)
    data["date"] = pd.to_datetime(data["date"])
    dataAll = data.groupby("ifcid")["value"].apply(list).reset_index(name="all")
    mean = data.groupby("ifcid")["value"].mean().reset_index()
    median = data.groupby("ifcid")["value"].median().reset_index()
    std = data.groupby("ifcid")["value"].std().reset_index()
    dates = data.groupby("ifcid")["date"].apply(list).reset_index(name="intervals")
    statsDf =pd.merge(pd.merge(pd.merge(pd.merge(mean, median, on="ifcid"),
                                        std, on="ifcid"),
                                        dataAll,on="ifcid"),
                                        dates,on="ifcid")
    return statsDf


def CreateWojewodztwoNodes(session):
    woj = gpd.read_file(r"D:\IIIrok\1sem\PAG\proj2\Dane\woj.shp").to_crs(epsg=2180)
    woj = woj[["name", "geometry", "national_c"]]
    for i in range(0, len(woj["name"])):
        print(f"{i+1}: {woj['name'][i]}")
        query = f"CREATE ( {str(woj['name'][i]).replace('-', '_')}:Wojewodztwo {{name:'{woj['name'][i]}',TERYT:'{woj['national_c'][i]}'}});"
        session.run(query)
        query = (
            'MATCH(a:Kraj) WHERE a.name = "Polska" MATCH(b:Wojewodztwo) WHERE b.name = "'
            + str(woj["name"][i])
            + '" CREATE (b)-[:JEST_W]->(a)'
        )
        session.run(query)


def CreatePowiatNodes(session):
    powiaty = gpd.read_file(r"D:\IIIrok\1sem\PAG\proj2\Dane\powiaty.shp").to_crs(
        epsg=2180
    )
    powiaty = powiaty[["name", "geometry", "national_c"]]
    for i in range(0, len(powiaty["name"])):
        print(f"{i+1}: {powiaty['name'][i]}")
        teryt = str(powiaty["national_c"][i])[0:2]
        query = (
            f"MATCH (a:Wojewodztwo {{TERYT:'{teryt}'}})"
            f"CREATE (:Powiat {{name:'{powiaty['name'][i]}', TERYT:'{powiaty['national_c'][i]}'}})-[:JEST_W]->(a);"
        )
        session.run(query)


def CreateEffacillitiesNodes(session):
    eff = gpd.read_file(r"Dane\stacje_meteo.geojson").to_crs(epsg=2180)
    eff = eff[["name1", "geometry", "ifcid", "pow"]]
    for i in range(0, len(eff["name1"])):
        print(f"{i+1}: {eff['name1'][i]}")
        station_name = re.sub(r"[- ().,/]", "_", str(eff["name1"][i]))
        query = (
            f"MATCH (a:Powiat {{name:'{eff['pow'][i]}'}}) "
            f"CREATE ({station_name}:Stacja {{name:'{eff['name1'][i]}', ifcid:'{eff['ifcid'][i]}'}})-[:JEST_W]->(a);"
        )
        session.run(query)


def createQuery(dataframe, i) -> str:
    query = (
        f"MATCH (a:Stacja {{ifcid:'{dataframe['ifcid'][i]}'}}) "
        f"CREATE (:DaneStatystyczne {{type:'Temperature',"
        f"allData:'{dataframe['all'][i]}',"
        f"mean:'{float(dataframe['value_x'][i])}',"
        f"truncMean:'{float(stats.trim_mean(dataframe['all'][i], 0.2))}',"
        f"median:'{float(dataframe['value_y'][i])}',"
        f"std:'{float(dataframe['value'][i])}'}})"
        f"-[:POMIERRZONE_PRZEZ]->(a);"
    )
    return query


def CreateDataNodes(session):
    tempGroundDf = ReadData(r"Dane\daneIMGW\B00305A_2023_09.csv")
    for i in range(0, len(tempGroundDf["ifcid"])):
        print(f"{i+1}: {tempGroundDf['ifcid'][i]}")
        query = createQuery(tempGroundDf, i)
        session.run(query)


def getStatsByStation(ifcid):
    query = f'MATCH (d:DaneStatystyczne)-[:POMIERRZONE_PRZEZ]->(s:Stacja WHERE s.ifcid="{ifcid}") return d.mean, d.median, d.truncMean, d.std;'
    data = session.run(query).values()
    if data != []:
        return data



def getStatsByPow(powiat):
    query = f'MATCH (s:Stacja)-[:JEST_W]->(p:Powiat WHERE p.name="{powiat}") RETURN s.ifcid;'
    stacje = session.run(query).value()
    pomiary = []
    for i in stacje:
        if getStatsByStation(i) != []:
            pomiary.append(getStatsByStation(i))
    pomiary = list(filter(None, pomiary))
    if pomiary != []:        
        pomiary = list(np.concatenate(pomiary))
    return pomiary


def getStatsByWoj(wojewodztwo):
    query = f'MATCH (p:Powiat)-[:JEST_W]->(w:Wojewodztwo WHERE w.name="{wojewodztwo}") RETURN p.name;'
    powiaty = session.run(query).value()
    pomiary = []
    for i in powiaty:
        pomiary.append(getStatsByPow(i))
    pomiary = list(filter(None, pomiary))
    return pomiary

if __name__ == "__main__":
    dotenv.load_dotenv("Neo4j-f5980f27-Created-2024-01-21.txt")
    URI = os.getenv("NEO4J_URI")
    AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))
    driver = GraphDatabase.driver(URI, auth=AUTH)
    session = driver.session()
    query = 'CREATE (:Kraj {name:"Polska"});'
    session.run(query)
    #CreateWojewodztwoNodes(session)
    #CreatePowiatNodes(session)
    #CreateEffacillitiesNodes(session)
    #CreateDataNodes(session)
    print(getStatsByPow("zamojski"))
    session.close()
    driver.close()
