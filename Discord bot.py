from audioop import ratecv
from time import time
from tkinter.tix import Tree
from turtle import back
from unittest import result
import discord
from fuzzywuzzy import fuzz
import betfairlightweight
import json
import requests
from discord.ext import commands, tasks
import datetime as dt
import numpy as np
import pytz
import pandas as pd
from tabulate import tabulate
import os
 
# Secrets
f= open('secrets/secrets.json')
secrets = json.load(f)
username = secrets["user_id"]
pw = secrets["bf_pw"]
api_key = secrets["API_KEY"]


#Key information
Token=secrets["token"]
channnel_id=secrets["channnel_id"]


# Logging details
payload = f'username={username}&password={pw}'
#Not clear why its needed, but auth breaks without
headers = {'X-Application': 'SomeKey', 'Content-Type': 'application/x-www-form-urlencoded'} 

# Key global variables, will be updated every 5 minutes
venue=None
race_no=None

resp = requests.post('https://identitysso-cert.betfair.com/api/certlogin', data=payload, cert=('adasd11.crt', 'client-2048.pem'), headers=headers)

if resp.status_code == 200:
    resp_json = resp.json()
    print(resp_json['loginStatus'])
    print(resp_json['sessionToken'])
else:
    print("Request failed.")
sessionToken = resp_json['sessionToken']


endpoint = "https://api.betfair.com/exchange/betting/rest/v1.0/"


# Different header than includes key
header = { 'X-Application' : api_key, 'X-Authentication' : sessionToken ,'content-type' : 'application/json' } 

account_url, url="https://api.betfair.com/exchange/betting/json-rpc/v1"



#Initializing key dataframes

bet_list_df=pd.read_excel("Bets Excel/race_day_bets.xlsx",sheet_name=None)
all_bet_list_df=pd.DataFrame()
for sheet in bet_list_df:
    all_bet_list_df=pd.concat([all_bet_list_df,bet_list_df[sheet]])

bet_record_df=pd.DataFrame(columns=["User","Venue","Race","Horse","Original Stake","Back Odds","Lay Type","Liability"])

venues=list(all_bet_list_df["Venue"].unique())


#Initializing discord bot
client = discord.Client()

# Permissions setting
approved_usersList=["adasd11","victorcd"]
sheet_dict={"adasd11":"Kelvin","victorcd":"Victor"}


#Sorting Timezones
UTC_zone=pytz.timezone('UTC')

time_now=dt.datetime.now(UTC_zone) 
recording_window=dt.timedelta(days=1)
time_end=time_now+recording_window
time_start=time_now-dt.timedelta(days=3)
time_startIso,time_nowIso,time_endIso=time_start.isoformat(),time_now.isoformat(), time_end.isoformat()

#Grabbing markets for today

def get_markets(comp_id:list,event_venues:list=venues) -> pd.DataFrame:
    jsonrpc_test=json.dumps(
                        [
                            {
                                "jsonrpc": "2.0",
                                "method": "SportsAPING/v1.0/listEvents",
                                "params": {
                                    "filter": {"eventTypeIds":comp_id,
                                                "marketStartTime":{"from":f"{time_startIso}",
                                                                    "to":f'{time_endIso}'},
                                                'venues':event_venues
                                                
                                    }            
                                },
                                "id": 1
                            }
                        ]
                            )

    #Getting json response
    response = requests.post(url, data=jsonrpc_test, headers=header)
    events_dict=dict(json.loads(response.text)[0])

    #Converting into dataframe
    result_dict=events_dict["result"]
    event_df=pd.DataFrame(columns=["Venue","EventId"])

    for event_dict in result_dict:
        #Cycles through venues and extracts event Id, concats to main df
        event_venue=event_dict['event']["venue"]
        event_Id=event_dict['event']["id"]

        event_temp_df=pd.DataFrame([[event_venue,event_Id]],columns=["Venue","EventId"])
        event_df=pd.concat([event_df,event_temp_df])
    event_df=event_df.reset_index(drop=True)
    return event_df

event_df=get_markets(["7"])

### Background supporting functions

def get_event_info(venue,race_no,market_type: str ="Win"):
    event_id=event_df[event_df["Venue"].str.lower()==venue.lower()]["EventId"].iloc[0]
    jsonrpc_test=json.dumps(
                        [
                            {
                                "jsonrpc": "2.0",
                                "method": "SportsAPING/v1.0/listMarketCatalogue",
                                "params": {
                                    "filter": {"eventIds":[f'{event_id}'],
                                                "marketStartTime":{"from":f"{time_nowIso}",
                                                                "to":f'{time_endIso}'},
                                                "marketTypeCodes":["WIN","PLACE"]
                                                                },
                                    'maxResults':100,
                                    "marketProjection": [
                                                            "COMPETITION",
                                                            "MARKET_START_TIME",
                                                            "MARKET_DESCRIPTION",
                                                            "RUNNER_METADATA"
                                                        ]
                                },
                                "id": 1
                            }
                        ]
                            )
    response = requests.post(url, data=jsonrpc_test, headers=header)
    races_list_dict=dict(json.loads(response.text)[0])
    race_df=pd.DataFrame.from_dict(races_list_dict["result"])
    event_winSeries=race_df[race_df["marketName"].str.contains(f"R{race_no}")]
    if market_type.lower()=="win":
        return event_winSeries
    else:
        #Place races are not properly named, so need to use start time as a reference
        time_reference=event_winSeries["marketStartTime"].iloc[0]
        temp_df=race_df[race_df["marketStartTime"]==time_reference]
        event_placeSeries=temp_df[temp_df["marketName"]=="To Be Placed"]
        return event_placeSeries


def get_betfair_commission(event_winSeries:pd.Series) -> float:
    """Takes a series gotten from get_event_info and grabs commission"""
    description_dict=dict(event_winSeries["description"].iloc[0])
    return description_dict['marketBaseRate']

def get_selection_id(horse,event_infoSeries):
    """Takes a series gotten from get_event_info and a horse name and grabs the selectionId"""
    runner_detailList=event_infoSeries['runners'].iloc[0]

    fuzzy_scoreList=[]
    for runnerDict in runner_detailList:
        runnerDict=dict(runnerDict)
        runner_nameStr=runnerDict['runnerName'].replace(" ",'').lower()[2:]
        if runner_nameStr == horse.replace(" ",'').lower():
            return runnerDict["selectionId"]
        #Also testing levenshtein distance to work around typos - complete later around choice parameter
        fuzzy_score=fuzz.partial_ratio(runner_nameStr, horse.replace(" ",'').lower())
        fuzzy_scoreList.append((fuzzy_score,runnerDict["selectionId"]))
    
    return None


def grab_lay_odds(venue,race_no,horse,market_type:str="Win") -> float:
    """Gets the current LAY odds for a horse, with a given venue and race_no"""
    event_infoSeries=get_event_info(venue,race_no,market_type)
    selection_id=get_selection_id(horse,event_infoSeries)
    marketId=event_infoSeries["marketId"].iloc[0]

    jsonrpc_test=json.dumps(
                            [
                                {
                                    "jsonrpc": "2.0",
                                    "method": "SportsAPING/v1.0/listMarketBook",
                                    "params": {
                                        "marketIds":[f'{marketId}'],
                                        'priceProjection':{"priceData":["EX_BEST_OFFERS"],
                                                            },
                                        'currencyCode':"AUD",
                                    },
                                    "id": 1
                                }
                            ]
                            )
    response = requests.post(url, data=jsonrpc_test, headers=header)
    price_dataDict=dict(json.loads(response.text)[0])
    runner_price_longDict=price_dataDict["result"][0]['runners']

    # Looking for the correct selection ID
    selection_id_valid=False
    for runner_dict in runner_price_longDict:
        runner_dict=dict(runner_dict)
        if runner_dict['selectionId']==selection_id:
            selection_id_valid=True
            runner_price_dict=runner_dict
    if not selection_id_valid:
        print(selection_id,runner_price_longDict)

    prices_dict=runner_price_dict['ex']
    lay_pricesList=prices_dict['availableToLay']
    lay_price=lay_pricesList[0]["price"]
    return lay_price

def bet(side,selectionid,marketid,backers_stake,odds):
    jsonrpc_test=json.dumps([
                            {
                                "jsonrpc": "2.0",
                                "method": "SportsAPING/v1.0/placeOrders",
                                "params": {
                                    "marketId": f"{marketid}",
                                    "instructions": [
                                        {
                                            "selectionId": f"{selectionid}",
                                            "handicap": "0",
                                            "side": side,
                                            "orderType": "LIMIT",
                                            "limitOrder": {
                                                "size": f"{backers_stake}",
                                                "price": f"{odds}",
                                                "persistenceType": "MARKET_ON_CLOSE"
                                            }
                                        }
                                    ]
                                },
                                "id": 1
                            }
                        ])
    response = requests.post(url, data=jsonrpc_test, headers=header)
    response_dict=dict(json.loads(response.text)[0])
    print(response_dict)
    if 'error' in response_dict:
        return "Something is Wrong"
    status=response_dict["result"]["status"]
    return status


def get_lay_backers_stake_and_profit(horse,market_type,strategy,stake,back_odds,venue,race_no):
    #Getting necessary market info
    market_detail=get_event_info(venue,race_no,market_type)
    lay_price=grab_lay_odds(venue,race_no,horse,market_type)
    commission=get_betfair_commission(market_detail)/100

    if strategy.lower() == "full_lay":
        #Calculating
        lay_liability=back_odds*stake/(1+((1-commission)/(lay_price-1)))
        lay_backers_stake=lay_liability/(lay_price-1)
        #Need to round to 2 decimal places or betfair is not fan
        lay_backers_stake=round(lay_backers_stake,2)
        expected_profit=(back_odds-1)*stake-lay_liability
        expected_profit=round(expected_profit,2)

        return lay_backers_stake,expected_profit
    
    elif strategy.lower() == "stake_lay":

        lay_backers_stake=stake*(1+commission)
        #Need to round to 2 decimal places or betfair is not fan
        lay_backers_stake=round(lay_backers_stake,2)
        lay_liability=lay_backers_stake*(lay_price-1)

        return lay_backers_stake,expected_profit


### Discord functions
def lay(horse,market_type,strategy,stake,back_odds,venue,race_no) -> dict:
    """Takes given info and lays horse at best available lay price. Returns dict in form {return:,win_profit:,loss_profit:,response:}"""
    #Getting necessary market info
    market_detail=get_event_info(venue,race_no,market_type)
    selection_id=get_selection_id(horse,market_detail)
    market_id=market_detail["marketId"].iloc[0]
    lay_price=grab_lay_odds(venue,race_no,horse,market_type)

    lay_backers_stake,expected_profit=get_lay_backers_stake_and_profit(horse,market_type,strategy,stake,back_odds,venue,race_no)

    if strategy.lower() == "full_lay":
        result=bet("LAY",selection_id,market_id,lay_backers_stake,lay_price)
        liability=lay_backers_stake*(lay_price-1)
        if result=="SUCCESS":
            responseStr= f"Full lay has been executed. Expect a profit of ${expected_profit:.2f} on win or lose."
            return {"result":result,"win_profit":expected_profit,'loss_profit':expected_profit,'response':responseStr}
        responseStr = f"There has been an error putting on your bet. Check the horse name or if there is enough liquidity"
        return {"result":result,"win_profit":expected_profit,'loss_profit':expected_profit,'liability':liability,'response':responseStr}


    elif strategy.lower() == "stake_lay":
        #First check to see if eligble
        if expected_profit < 0:
            #Recalcs lay_backers stake as a full lay

            lay_backers_stake,expected_profit=get_lay_backers_stake_and_profit(horse,market_type,"full_lay",stake,back_odds,venue,race_no)
            liability=lay_backers_stake*(lay_price-1)
            result=bet("LAY",selection_id,market_id,lay_backers_stake,lay_price)
            if result=="SUCCESS":
                responseStr= f"Stake lay illegal - lay price is higher than back price. Full lay has been executed instead. Expect a profit of ${expected_profit:.2f} on win or lose."
                return {"result":result,"win_profit":expected_profit,'loss_profit':0,'response':responseStr}
            responseStr = f"There has been an error putting on your bet. Check the horse name or if there is enough liquidity"
            return {"result":result,"win_profit":expected_profit,'loss_profit':expected_profit,'liability':liability,'response':responseStr}
        
        lay_backers_stake,expected_profit=get_lay_backers_stake_and_profit(horse,market_type,strategy,stake,back_odds,venue,race_no)
        liability=lay_backers_stake*(lay_price-1)
        result=bet("LAY",selection_id,market_id,lay_backers_stake,lay_price)
        if result=="SUCCESS":
            responseStr= f"Stake lay has been executed. Expect a profit of ${expected_profit:.2f} on win - 0 loss on lose."
            return {"result":result,"win_profit":expected_profit,'loss_profit':0,'response':responseStr}
        responseStr = f"There has been an error putting on your bet. Check the horse name or if there is enough liquidity"
        return {"result":result,"win_profit":expected_profit,'loss_profit':0,'liability':liability,'response':responseStr}

    else:
        responseStr= "Strategy not recognized. The two valid lay strategies are Full_Lay or Stake_Lay. Underscore is mandatory"
        return {"result":"NA_STRAT","win_profit":np.nan,'loss_profit':np.nan,'liability':np.nan,'response':responseStr}


def details(placeholder):
    jsonrpc_test=json.dumps(
                        [
                            {
                                "jsonrpc": "2.0",
                                "method": "AccountAPING/v1.0/getAccountFunds",

                                "id": 1
                            }
                        ]
                            )
    response = requests.post(account_url, data=jsonrpc_test, headers=header)
    responseTree=dict(json.loads(response.text)[0])
    liquidity=responseTree['result']['availableToBetBalance']
    exposure=responseTree['result']['exposure']
    return f"Remaining liquidity: ${liquidity}.\n You have already bet: ${exposure}"

def get_bets(placeholder):
    tableStr=tabulate([list(row[1].values) for row in all_bet_list_df.head(5).iterrows()],headers=all_bet_list_df.columns, tablefmt='pretty')
    return "Table of bets: \n"+tableStr.replace(" ","  ")

def get_next_bets(user):
    global venue
    global race_no
    if venue == None:
        return "No race in the next 15 min"
    # tableStr=tabulate([list(row[1].values) for row in next_bet_list_df.iterrows()],headers=next_bet_list_df.columns, tablefmt='pretty')

    infoStr=""
    for row in next_bet_list_df[user].iterrows():
        back_odds=row[1]["Back Odds"]
        backers_stake,stake_lay_win=get_lay_backers_stake_and_profit(horse,market_type,"Stake_lay",stake,back_odds,venue,race_no)
        backers_stake,full_lay_win=get_lay_backers_stake_and_profit(horse,market_type,"Full_lay",stake,back_odds,venue,race_no)
        horse=row[1]["Horse"]
        market_type=row[1]["Market Type"]
        stake=row[1]["Stake"]
        infoStr += f"Horse: {horse}, Market: {market_type}, Stake: {stake}, Stake Lay Win: {stake_lay_win}, Stake Lay Win: {full_lay_win} \n"
    return "Table of bets: \n"+infoStr


def next_race(placeholder):
    global venue
    global race_no
    if race_no == None:
        return "No race in the next 15 min"
    return f"Next race is race {race_no} at {venue}"

def help(placeholder):
    string="""Commands are:
    ? Details
    Returns current liability and liquidity. \n
    ? Get_Bets
    Gives table of the next 5 bets \n
    ? Get_Next_Bets
    Gives table of all bets in next race as well as lay results \n
    ? Next_Race
    Returns the next race that it is accepting lays on. \n
    ! Lay horse market_type strat
    Lays a horse provided its in the next race. Market_type is win or place. 
    Two strats - full_lay which lays evenly
    and stake_lay which lays only the stake.
    Example: ! Lay Marabi Win stake_lay \n
    ! Partial_Lay horse stake market_type strat
    Lays a horse provided its in the next race. Market_type is win or place. 
    Two strats - full_lay which lays evenly
    and stake_lay which lays only the stake.
    Example: ! Lay Marabi Win stake_lay \n
    ! Lay_all strat
    Lays all stakes for the next race using the same strat (full_lay and stake_lay as above)
    Example: ! Lay_all stake_lay
    

    """
    return string

was_venue_noneBool=True

@tasks.loop(minutes=3)
async def set_clock_1():
    global venue
    global race_no
    global was_venue_noneBool
    print(was_venue_noneBool)
    channel = client.get_channel(channnel_id)
    print(channel)

    #Getting unique combos of venues and races
    race_list_df=all_bet_list_df.loc[:,["Venue","Race"]].drop_duplicates().reset_index(drop=True).sort_values("Race")

    
    UTC_zone=pytz.timezone('UTC')
    time_now=dt.datetime.now(UTC_zone) 

    time_dif_Df=pd.DataFrame(columns=["Venue","Race_No","Time_Dif"])
    for row in race_list_df.iterrows():
        test_venue=row[1]["Venue"]
        test_race_no=row[1]["Race"]
        test_eventinfoSeries=get_event_info(test_venue,test_race_no)
        
        #Getting the race start time
        if len(test_eventinfoSeries)==0:
            continue
        test_race_time=test_eventinfoSeries["marketStartTime"].iloc[0]
        test_race_time=dt.datetime.fromisoformat(test_race_time[:-1]) 
        test_race_time=UTC_zone.localize(test_race_time)

        time_dif=test_race_time-time_now
        temp_time_dif_Df=pd.DataFrame([[test_venue,test_race_no,time_dif]],columns=["Venue","Race_No","Time_Dif"])
        time_dif_Df=pd.concat([time_dif_Df,temp_time_dif_Df] )

    if len(time_dif_Df)==0:
        return
    time_dif_Df=time_dif_Df.reset_index(drop=True)
    time_dif_Df=time_dif_Df.sort_values('Time_Dif',ascending=True)

    for i in range(len(time_dif_Df)):
        if time_dif_Df.loc[i,"Time_Dif"]>dt.timedelta(seconds=0) and time_dif_Df.loc[i,"Time_Dif"]<dt.timedelta(seconds=960):
            venue=time_dif_Df.loc[i,"Venue"]
            race_no=time_dif_Df.loc[i,"Race_No"]
            
            # Defines the global next_bet_list_df
            global next_bet_list_df
            print(was_venue_noneBool)
            if was_venue_noneBool:
                next_bet_list_df=bet_list_df.copy()
                for sheet in next_bet_list_df:
                    #Creating a temp
                    individual_bets=next_bet_list_df[sheet]
                    next_bet_list_df[sheet]=individual_bets[(individual_bets["Venue"]==venue) & (individual_bets["Race"]==race_no)]
                was_venue_noneBool=False 
                await channel.send(f"Next race is race {race_no} at {venue}")
            break
        else:
            was_venue_noneBool=True
            venue=None
            race_no=None

    #Extra stuff not related to figuring out the next race
    
    bet_record_df.to_excel("Bet_Output/Arbing_Results.xlsx",index=False)


    resp = requests.post('https://identitysso-cert.betfair.com/api/certlogin', data=payload, cert=('adasd11.crt', 'client-2048.pem'), headers=headers)

    if resp.status_code == 200:
        resp_json = resp.json()
        print(resp_json['loginStatus'])
        print(resp_json['sessionToken'])
    else:
        print("Request failed.")
    global sessionToken
    global header
    header = { 'X-Application' : api_key, 'X-Authentication' : sessionToken ,'content-type' : 'application/json' }
    sessionToken = resp_json['sessionToken']

@client.event
async def on_ready():
    print('We have logged in as {0.user}'.format(client))
    UTC_zone=pytz.timezone('UTC')
    time_now=dt.datetime.now(UTC_zone) 
    sleep_timer=5-time_now.minute%5
    when=time_now+dt.timedelta(seconds=sleep_timer*60)
    # await discord.utils.sleep_until(when, result=None)
    set_clock_1.start()




@client.event
async def on_message(message):
    username = str(message.author).split('#')[0]
    user_message=str(message.content)
    channel=str(message.channel.name)
    sheet_name=sheet_dict[username]
    individual_bet_list_df=next_bet_list_df[sheet_name]
    print(f'{username}: {user_message} ({channel})')

    if not username in approved_usersList:
        return

    # if not user_message[0] in ["!","?"]:
    #     return

    if user_message[0]=="?":
        func_name=user_message.split()[1].lower()
        if not func_name in globals():
            await message.channel.send("Unkown command. Type ? Help for commands")
        messageStr=globals()[func_name](sheet_name)
        await message.channel.send(messageStr)
        return

    if user_message[0]=="!":
        global bet_list_df
        
        variables=user_message.split()[1:]
        if variables[0].lower()=='lay':
            if len(variables) != 4:
                await message.channel.send("Missing/too many arguments. Use _ for horses with a space in their name")
                return 

            horse=variables[1].replace("_"," ")
            market_type=variables[2]
            strat=variables[3]
            if venue == None:
                await message.channel.send("No race in the next 15 min")
                return 


            print(individual_bet_list_df,horse,strat)
            horse_df=individual_bet_list_df[(individual_bet_list_df["Horse"].str.lower()==horse.lower()) & (individual_bet_list_df["Market Type"].str.lower()==market_type.lower())]
            if len(horse_df)==0:
                await message.channel.send(f"{horse} or {market_type} not valid. Check your spelling")
                return 

            index_to_change=horse_df.index[0]
                
            if horse_df["Layed"].iloc[0]=="Yes":
                await message.channel.send(f"Already layed {horse} for the {market_type}!")
                return 

            stake=horse_df["Stake"].iloc[0]
            back_odds=horse_df["Back Odds"].iloc[0]  
            response_dict=lay(horse,market_type,strat,stake,back_odds,venue,race_no)
        
            if response_dict['result'] != "SUCCESS":
                await message.channel.send(response_dict['response'])
                return
            
            individual_bet_list_df.loc[index_to_change,"Win Profit"]=response_dict["win_profit"]
            individual_bet_list_df.loc[index_to_change,"Loss Profit"]=response_dict["loss_profit"]
            individual_bet_list_df.loc[index_to_change,"Layed"]="Yes"

            await message.channel.send(response_dict['response'])
            # Merging to original data frame
            full_bet_listSeries=bet_list_df[sheet_name][(bet_list_df[sheet_name]["Horse"].str.lower()==horse.lower()) & (bet_list_df[sheet_name]["Market Type"].str.lower()==market_type.lower())]
            full_bet_listIndex=full_bet_listSeries.index[0]

            bet_list_df[sheet_name].loc[full_bet_listIndex,"Win Profit"]=response_dict["win_profit"]
            bet_list_df[sheet_name].loc[full_bet_listIndex,"Loss Profit"]=response_dict["loss_profit"]
            bet_list_df[sheet_name].loc[full_bet_listIndex,"Layed"]="Yes"
            
            liability=response_dict["liability"]
            #Recording bet info
            bet_record_row=pd.DataFrame([[sheet_name,venue,race_no,horse,stake,back_odds,strat,liability]],columns=["User","Venue","Race","Horse","Original Stake","Back Odds","Lay Type","Liability"])
            bet_record_df=pd.concat([bet_record_df,bet_record_row]).reset_index(drop=True)
            print("Successfully_merged")
            return
        
        elif variables[0].lower()=='lay_all':
            strat=variables[1]
            for row in individual_bet_list_df.iterrows():
                
                index_to_change=row[0]
                horse_Series=row[1]
                horse=horse_Series["Horse"]
                stake=horse_Series["Stake"]
                back_odds=horse_Series["Back Odds"]
                market_type=horse_Series["Market Type"]
                
                if horse_Series["Layed"]=="Yes":
                    await message.channel.send(f"Already layed {horse} for the {market_type}!")
                    continue

                response_dict=lay(horse,market_type,strat,stake,back_odds,venue,race_no)

                if response_dict['result'] != "SUCCESS":
                    await message.channel.send(response_dict['response'])
                    continue
                individual_bet_list_df.loc[index_to_change,"Win Profit"]=response_dict["win_profit"]
                individual_bet_list_df.loc[index_to_change,"Loss Profit"]=response_dict["loss_profit"]
                individual_bet_list_df.loc[index_to_change,"Layed"]="Yes"

                await message.channel.send(response_dict['response'])

                # Merging to original data frame
                full_bet_listSeries=bet_list_df[sheet_name][(bet_list_df[sheet_name]["Horse"].str.lower()==horse.lower()) & (bet_list_df[sheet_name]["Market Type"].str.lower()==market_type.lower())]
                full_bet_listIndex=full_bet_listSeries.index[0]

                bet_list_df[sheet_name].loc[full_bet_listIndex,"Win Profit"]=response_dict["win_profit"]
                bet_list_df[sheet_name].loc[full_bet_listIndex,"Loss Profit"]=response_dict["loss_profit"]
                bet_list_df[sheet_name].loc[full_bet_listIndex,"Layed"]="Yes"

                liability=response_dict["liability"]
                #Recording bet info
                bet_record_row=pd.DataFrame([[sheet_name,venue,race_no,horse,stake,back_odds,strat,liability]],columns=["User","Venue","Race","Horse","Original Stake","Back Odds","Lay Type","Liability"])
                bet_record_df=pd.concat([bet_record_df,bet_record_row]).reset_index(drop=True)
                print("Successfully_merged")
            return

        elif variables[0].lower()=='partial_lay':
            if len(variables) != 5:
                await message.channel.send("Missing/too many arguments. Use _ for horses with a space in their name")
                return 

            horse=variables[1].replace("_"," ")
            partial_stake=variables[2]
            market_type=variables[3]
            strat=variables[4]
            
            if venue == None:
                await message.channel.send("No race in the next 15 min")
                return 


            print(individual_bet_list_df,horse,strat)
            horse_df=individual_bet_list_df[(individual_bet_list_df["Horse"].str.lower()==horse.lower()) & (individual_bet_list_df["Market Type"].str.lower()==market_type.lower())]
            if len(horse_df)==0:
                await message.channel.send(f"{horse} or {market_type} not valid. Check your spelling")
                return 

            index_to_change=horse_df.index[0]
                
            if horse_df["Layed"].iloc[0]=="Yes":
                await message.channel.send(f"Already layed {horse} for the {market_type}!")
                return 

            back_odds=horse_df["Back Odds"].iloc[0]  
            original_stake=horse_df["Stake"].iloc[0]
            response_dict=lay(horse,market_type,strat,partial_stake,back_odds,venue,race_no)
            if response_dict['result'] != "SUCCESS":
                await message.channel.send(response_dict['response'])
                return
            
            individual_bet_list_df.loc[index_to_change,"Win Profit"]=response_dict["win_profit"]
            individual_bet_list_df.loc[index_to_change,"Loss Profit"]=response_dict["loss_profit"]
            individual_bet_list_df.loc[index_to_change,"Layed"]="Yes"

            await message.channel.send(response_dict['response'])
            # Merging to original data frame
            full_bet_listSeries=bet_list_df[sheet_name][(bet_list_df[sheet_name]["Horse"].str.lower()==horse.lower()) & (bet_list_df[sheet_name]["Market Type"].str.lower()==market_type.lower())]
            full_bet_listIndex=full_bet_listSeries.index[0]

            bet_list_df[sheet_name].loc[full_bet_listIndex,"Win Profit"]=response_dict["win_profit"]
            bet_list_df[sheet_name].loc[full_bet_listIndex,"Loss Profit"]=response_dict["loss_profit"]
            bet_list_df[sheet_name].loc[full_bet_listIndex,"Layed"]="Yes"

            
            liability=response_dict["liability"]
            #Recording bet info
            bet_record_row=pd.DataFrame([[sheet_name,venue,race_no,horse,original_stake,back_odds,strat,liability]],columns=["User","Venue","Race","Horse","Original Stake","Back Odds","Lay Type","Liability"])
            bet_record_df=pd.concat([bet_record_df,bet_record_row]).reset_index(drop=True)
            print("Successfully_merged")
            return
        else:
            await message.channel.send("Don't recognize lay command. Only ! commands are lay and lay_all")
            return
    
    if message.channel.name == 'general':
        if user_message.lower() =='hello':
            await message.channel.send(f'Hello {username}')
            return
        elif user_message.lower() == 'bye':
            await message.channel.send(f'See you {username}!')
            return

client.run(Token)