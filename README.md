# ebs_project_2023
Event based systems

```
			  PUB
			  /|\

		B1 - - - - B2 - - - - B3             ←—- simple routing algorithm

               \|/         \|/          \|/    
		S1          S2          S3
```


### How to run

```bash
# Start RabbitMQ nodes (clustered)
./config/start_rmq.sh

# (To stop rabbits:   ./config/stop_rmq.sh)

# Start brokers (in separate console)
python brokers.py

# Send subscriptions (in separate console)
#   -n argument for number of subscriptions
python send_subscriptions.py -n 1000

# Start subscribers (waiting to receive notifications) (in separate console)
python subscribers.py

# Start sending notifications (in separate console)
#   -s argument for number of seconds (sending at circa 6k notifications per second)
python publisher.py -s 0.1


```

Node logic & routing logic inside `rabbit.py` (simple routing & subscription matching).


(Partial report: https://docs.google.com/document/d/1NJd4FBJmbbTTnvgH5mXBbBHhNYEe1-rA9Yv9YzHBdyQ/edit?usp=sharing)


### Task description

Scrieti un program care sa genereze aleator seturi echilibrate de subscriptii si publicatii cu posibilitatea de fixare a: numarului total de mesaje (publicatii, respectiv subscriptii), ponderii pe frecventa campurilor din subscriptii si ponderii operatorilor de egalitate din subscriptii pentru cel putin un camp. Publicatiile vor avea o structura fixa de campuri. Implementarea temei va include o posibilitate de paralelizare pentru eficientizarea generarii subscriptiilor si publicatiilor, si o evaluare a timpilor obtinuti.

Exemplu de structurare a detelor:
- **Publicatie**: `{(stationid,1);(city,"Bucharest");(temp,15);(rain,0.5);(wind,12);(direction,"NE");(date,2.02.2023)}`
- Structura fixa a campurilor publicatiei e: stationid-integer, city-string, temp-integer, rain-double, wind-integer, direction-string, date-data;
- pentru anumite campuri (stationid, city, direction, date), se pot folosi **seturi de valori prestabilite de unde se va alege una la intamplare**;
- pentru celelalte campuri se pot stabili **limite inferioare si superioare intre care se va alege una la intamplare**.

- **Subscriptie**: `{(city,=,"Bucharest");(temp,>=,10);(wind,<,11)}`
- Unele campuri pot lipsi; frecventa campurilor prezente trebuie sa fie configurabila (ex. 90% city - exact 90% din subscriptiile generate, cu eventuala rotunjire la valoarea cea mai apropiata de procentul respectiv, trebuie sa includa campul "city");
- pentru cel putin un camp (exemplu - city) trebui sa se poate configura un minim de frecventa pentru operatorul "=" (ex. macar 70% din subscriptiile generate sa aiba ca operator pe acest camp egalitatea).

Note:
- cazul in care suma procentelor configurate pentru campuri e mai mica decat 100 reprezinta o situatie de exceptie care nu e necesar sa fie tratata (pentru testare se vor folosi intotdeauna valori de procentaj ce sunt egale sau depasesc 100 ca suma)
- tema cere doar generarea de date, nu implementarea unei topologii Storm care sa includa functionalitatea de matching intre subscriptii si publicatii; nu exista restrictii de limbaj sau platforma pentru implementare
- pentru optimizarea de performanta prin paralelizare pot fi considerate fie threaduri (preferabil) sau o rulare multiproces pentru generarea subscriptiilor si publicatiilor; se va lua in calcul posibila necesitate de sincronizare a implementarii paralelizate ce poate sa apara in functie de algoritmul ales pentru generarea subscriptiilor
- evaluarea implementarii va preciza in fisierul "readme" asociat temei urmatoarele informatii: tipul de paralelizare (threads/procese), factorul de paralelism (nr. de threads/procese) - se cere executia pentru macar doua valori de test comparativ, ex. 1 (fara paralelizare) si 4, numarul de mesaje generat, timpii obtinuti si specificatiile procesorului pe care s-a rulat testul.

Hint: NU se recomanda utilizarea distributiei random in obtinerea procentelor cerute pentru campurile subscriptiilor (nu garanteaza o distributie precisa).

Setul de date generat va fi memorat in fisiere text. Tema se poate realiza in echipe de pana la 3 studenti. 


### Usage and results


#### Requirements

Python `>=3.9`.

```bash
mamba install py-cpuinfo
# OR
pip install py-cpuinfo
```

#### Subscription generation results

```bash

$ python test_sub_gen.py
```

```bash
Testing subscription generator.

I. Testing platform information:
		python_version: 3.9.16.final.0 (64 bit)
		arch: ARM_8
		bits: 64
		count: 10
		arch_string_raw: arm64
		brand_raw: Apple M1 Pro
	Parameters:
	{'city': 0.2, 'temp': 0.71, 'station_id': 0.955}
	{('city', '='): 0.5, ('station_id', '='): 0.5}

	Testing MULTIPROCESSING work on num workers: 5
	Generated 1000000 subscriptions over 5 workers
	Duration: 2.831
	Rate of subscriptions per second: 353286.9
	----------------------------------------------------------------------------------------------------
	Total subscriptions: 1000000
	Field name           Counts     Op '='     Percentage      Op '=' percentage
	----------------------------------------------------------------------------------------------------
	city                 200000     150099     20.0            75.0      
	date                 1000000    166788     100.0           17.0      
	station_id           955000     557193     95.0            57.99999999999999
	temp                 710000     118473     71.0            17.0      
	wind                 1000000    166621     100.0           17.0      
	wind_direction       1000000    499034     100.0           50.0      
	----------------------------------------------------------------------------------------------------
	[(('city', '=', 'Berne'), ('temp', '>=', 34.55), ('wind', '<', 25.5), ('wind_direction', '=', 'V'), ('date', '!=', datetime.datetime(2020, 1, 7, 11, 54, 54))),
	(('temp', '!=', 33.45), ('wind', '<=', 68.55), ('wind_direction', '=', 'SE'), ('date', '<=', datetime.datetime(2022, 12, 11, 22, 59, 40)))]
	----------------------------------------------------------------------------------------------------

	Testing MULTITHREADING work on num workers: 5
	Generated 1000000 subscriptions over 5 workers
	Duration: 8.929
	Rate of subscriptions per second: 111995.6
	----------------------------------------------------------------------------------------------------
	Total subscriptions: 1000000
	Field name           Counts     Op '='     Percentage      Op '=' percentage
	----------------------------------------------------------------------------------------------------
	city                 200000     149726     20.0            75.0      
	date                 1000000    166628     100.0           17.0      
	station_id           955000     557290     95.0            57.99999999999999
	temp                 710000     118068     71.0            17.0      
	wind                 1000000    166266     100.0           17.0      
	wind_direction       1000000    501157     100.0           50.0      
	----------------------------------------------------------------------------------------------------
	[(('station_id', '<=', 72), ('temp', '=', -0.15), ('wind', '>=', 89.2), ('wind_direction', '=', 'E'), ('date', '>=', datetime.datetime(2022, 3, 21, 5, 14, 35))),
	(('station_id', '=', 74), ('wind', '<=', 20.15), ('wind_direction', '=', 'SV'), ('date', '=', datetime.datetime(2020, 10, 2, 7, 29, 58)))]



II. Testing platform information:
			python_version: 3.9.6.final.0 (64 bit)
			arch: ARM_8
			bits: 64
			count: 10
			arch_string_raw: arm64
			brand_raw: Apple M1 Pro
	Parameters:
	{'city': 0.2, 'temp': 0.71, 'station_id': 0.955}
	{('city', '='): 0.5, ('station_id', '='): 0.5}

	Testing MULTIPROCESSING work on num workers: 5
	Generated 1000000 subscriptions over 5 workers
	Duration: 3.416
	Rate of subscriptions per second: 292742.5
	----------------------------------------------------------------------------------------------------
	Total subscriptions: 1000000
	Field name           Counts     Op '='     Percentage      Op '=' percentage
	----------------------------------------------------------------------------------------------------
	city                 200000     150043     20.0            75.0      
	date                 1000000    166745     100.0           17.0      
	station_id           955000     557220     95.0            57.99999999999999
	temp                 710000     118944     71.0            17.0      
	wind                 1000000    167119     100.0           17.0      
	wind_direction       1000000    500763     100.0           50.0      
	----------------------------------------------------------------------------------------------------
	[(('station_id', '=', 5), ('temp', '=', 16.5), ('wind', '<', 89.0), ('wind_direction', '!=', 'E'), ('date', '<=', datetime.datetime(2023, 3, 17, 8, 53, 30))), (('station_id', '!=', 22), ('wind', '>=', 27.15), ('wind_direction', '=', 'E'), ('date', '>=', datetime.datetime(2022, 11, 13, 7, 36, 43)))]
	----------------------------------------------------------------------------------------------------

	Testing MULTITHREADING work on num workers: 5
	Generated 1000000 subscriptions over 5 workers
	Duration: 11.140
	Rate of subscriptions per second: 89763.1
	----------------------------------------------------------------------------------------------------
	Total subscriptions: 1000000
	Field name           Counts     Op '='     Percentage      Op '=' percentage
	----------------------------------------------------------------------------------------------------
	city                 200000     149973     20.0            75.0      
	date                 1000000    166279     100.0           17.0      
	station_id           955000     557401     95.0            57.99999999999999
	temp                 710000     118171     71.0            17.0      
	wind                 1000000    166357     100.0           17.0      
	wind_direction       1000000    499228     100.0           50.0      
	----------------------------------------------------------------------------------------------------
	[(('station_id', '=', 82), ('temp', '!=', -20.25), ('wind', '!=', 23.05), ('wind_direction', '=', 'SV'), ('date', '>=', datetime.datetime(2022, 3, 15, 14, 24, 55))), (('station_id', '=', 21), ('temp', '<', 26.75), ('wind', '=', 93.05), ('wind_direction', '=', 'S'), ('date', '<', datetime.datetime(2022, 7, 9, 11, 7, 57)))]
```
