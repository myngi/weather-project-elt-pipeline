{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "fd37ad90",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "{'timestamp': '2025-12-02 12:20:00+00:00',\n",
       " 'location': 'Oulu Vihreäsaari satama',\n",
       " 'source': 'FMI',\n",
       " 'latitude': 65.00637,\n",
       " 'longitude': 25.39325,\n",
       " 'temperature': -0.1}"
      ]
     },
     "execution_count": 2,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "import pandas as pd\n",
    "import fmi_weather_client as fmi\n",
    "\n",
    "FMISID = 101794 #Example, OUlu, Vihreäsaaro\n",
    "weather = fmi.observation_by_station_id(FMISID)\n",
    "if weather is not None:\n",
    "    message = {\n",
    "    \"timestamp\": str(pd.to_datetime(weather.data.time)),\n",
    "    \"location\": weather.place,\n",
    "    \"source\": \"FMI\",\n",
    "    'latitude': weather.lat,\n",
    "    'longitude': weather.lon,\n",
    "    \"temperature\": weather.data.temperature.value\n",
    "    }\n",
    "     \n",
    "\n",
    "message\n"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.12.3"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}