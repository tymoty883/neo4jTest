from datetime import datetime

import geopandas as gpd
import pandas as pd
from shapely import to_geojson

def create_main_dataframes() -> gpd.GeoDataFrame:
    powiaty = create_powiaty()
    woj = create_wojewodztwa()
    effacility = create_effacility()

    dataAll = pd.read_csv(r"Dane\daneIMGW\B00604S_2023_09.csv",
                                header=None,
                                delimiter=";").rename(columns={0: "ifcid", 1: "type", 2: "date", 3: "value"})
    
    dataAll['value'] = dataAll['value'].str.replace(',', '.').astype(float)
    dataAll["date"] = pd.to_datetime(dataAll["date"])

    # Przygotowanie do analizy geostat
    stacje_zlaczone = dataAll.merge(effacility[["ifcid", "geometry", "name1"]], how="left", on="ifcid")
    stacje_zlaczone = gpd.GeoDataFrame(stacje_zlaczone, geometry=stacje_zlaczone["geometry"]).to_crs(epsg=2180)
    
    stacje_zlaczone = gpd.tools.sjoin(stacje_zlaczone, woj[["geometry", "name"]], how="left", rsuffix="woj")
    stacje_zlaczone = gpd.tools.sjoin(stacje_zlaczone, powiaty[["geometry", "name"]], how="left", rsuffix="pow").rename(
        columns={"name_left": "name_woj", 'name1':"name_eff"})
    stacje_zlaczone = stacje_zlaczone.drop(columns=[4])
    stacje_zlaczone = stacje_zlaczone.dropna()
    
    return dataAll, stacje_zlaczone, effacility, powiaty, woj

def create_effacility() -> gpd.GeoDataFrame:
    eff = gpd.read_file(r"C:\Users\qattr\Desktop\STUD\SEM 5\PAG\Projekt-2\Dane\effacility.geojson")
    eff.crs = 2180
    return eff

def create_wojewodztwa() -> gpd.GeoDataFrame:
    woj = gpd.read_file(r"C:\Users\qattr\Desktop\STUD\SEM 5\PAG\Projekt-2\Dane\woj.shp").to_crs(epsg=2180)
    woj = woj[['name', 'geometry']]
    return woj


def create_powiaty() -> gpd.GeoDataFrame:
    powiaty = gpd.read_file(r"C:\Users\qattr\Desktop\STUD\SEM 5\PAG\Projekt-2\Dane\powiaty.shp").to_crs(epsg=2180)
    powiaty = powiaty[['name', 'geometry']]
    return powiaty

# *** ANALIZA STATYSTYCZNA

def create_dataframes(main_dataframe: pd.DataFrame):
    opady_daytime = main_dataframe.loc[main_dataframe['is_day'] == True].groupby(pd.Grouper(key='date', freq='D'))['value'].aggregate(["mean", "median"])
    opady_nighttime = main_dataframe.loc[main_dataframe['is_day'] == False].groupby(pd.Grouper(key='date', freq='D'))['value'].aggregate(["mean", "median"])
    opady_day = main_dataframe.groupby(pd.Grouper(key='date', freq='D'))['value'].aggregate(["mean", "median"])
    # add maybe also group by effacility?
    
    # Średnia odcięta kod, który nie działa z aggregate: scipy.stats.trim_mean(main_dataframe.loc[main_dataframe['is_day'] == True].value, 0.1)
    return opady_daytime, opady_nighttime, opady_day 


# *** ANALIZA GEOSTATYSTYCZNA

def create_geodataframes(main_geodataframe: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    
    # Srednia i mediana wartości pomiaru w podziale na daty w poszczegolnych wojewodztwach i powiatach:
    opady_woj = main_geodataframe.groupby(["name_woj", "date"])["value"].aggregate(["mean", "median"])
    opady_pow = main_geodataframe.groupby(["name_pow", "date"])["value"].aggregate(["mean", "median"])

    return opady_woj, opady_pow

def calculate_stat_change(gdf: gpd.GeoDataFrame, gov_unit_name: str, date_start: datetime, date_end: datetime):
    """
    Args:
        gdf (gpd.GeoDataFrame): result of create_geodataframes function GeoDataFrame containing gov_unit of interest
        date_start (datetime): Format: "yyyy-mm-dd HH:MM" - has to match date and time in gdf index
        date_end (datetime): Format: "yyyy-mm-dd HH:MM" 
    """
    date_format = "%Y-%m-%d %H:%M"
    time_start = datetime.strptime(date_start, date_format)
    time_end = datetime.strptime(date_end, date_format)
    
    xs = pd.IndexSlice
    try:
        mean_end = gdf.loc[xs[gov_unit_name, time_end], "mean"]
        mean_start = gdf.loc[xs[gov_unit_name, time_start], "mean"]
        
        median_end = gdf.loc[xs[gov_unit_name, time_end], "median"]
        median_start = gdf.loc[xs[gov_unit_name, time_start], "median"]
        
        print(f"***\nJednostka: {gov_unit_name}\nDaty: {date_start}, {date_end}\nRóżnica średniej: {mean_end - mean_start}\nRóżnica mediany: {median_end - median_start}")
        return mean_end - mean_start, median_end - median_start
    except:
        print("! | ERR: calc_stat_change")
        return