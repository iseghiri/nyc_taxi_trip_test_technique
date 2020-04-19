# Étude de cas
## Présentation
Nous souhaitons étudier le comportement des trajets des taxis new yorkais. Pour cela nous
vous demandons de calculer les indicateurs ci-dessous :
- la vitesse moyenne de chaque trajet,
- le nombre de trajets effectués en fonction du jour de la semaine,
- le nombre de trajets effectués en fonction de l’horaire de la journée par tranche de 4h,
- le nombre de km parcourus par jour de la semaine.

## Data
Les données et leurs descriptions sont sur le lien ci-dessous :
https://www.kaggle.com/c/nyc-taxi-trip-duration/data
Pour cette étude il est nécessaire de télécharger uniquement le fichier : train.csv (200 Mo)
### Descriptions des données :
- id - identifiant unique pour chaque trajet
- vendor_id - un code indiquant le transporteur associé à l'enregistrement du trajet
- pickup_datetime - date et heure du lancement du compteur
- dropoff_datetime - date et heure d'arrêt du compteur
- passenger_count - nombre de passagé dans le vehicule
- pickup_longitude - longitude du point de départ
- pickup_latitude - latitude du point de départ
- dropoff_longitude - longitude du point d'arrivé 
- dropoff_latitude - latitude du point d'arrivé
- store_and_fwd_flag - Ce flag indique si l'enregistrement du trajet a été conservé dans la mémoire du véhicule - - avant d'être envoyé au vendeur parce que le véhicule n'avait pas de connexion au serveur - - Y=stocké et envoyé ; N=non stoké et envoyé
- trip_duration - duré du trajet en seconde

## Rendu
Un projet complet “production ready” écrit en python ou scala et utilisant de préférence les API
Spark. Nous attendons en livrable un zip contenant un dépôt git, avec différents commits, un
peu de documentation et des tests ! Ne pas inclure le gros csv, juste un extrait avec 1000 lignes
devrait suffire.


## Commentaires 

### Calcul de la vitesse 

J'ai utilisé ici la formule de Haversine pour cacluler la distance en km entre un point A et un point B.
<img src="https://render.githubusercontent.com/render/math?math=a = sin^2(\frac{lat_B-lat_A}{2})+ cos(lat_A)*cos(lat_B)*sin^2(\frac{long_B-long_A}{2})">


<img src="https://render.githubusercontent.com/render/math?math=d=2*r*arcsin(\sqrt{a})">


avec r le rayon de la sphère sur laquelle sont placés les points. On considérera ici la terre comme sphérique, ce qui donne de bon résultat en pratique.
D'après la formule de wikipédia : https://en.wikipedia.org/wiki/Haversine_formula


On sait ensuite que :

<img src="https://render.githubusercontent.com/render/math?math=vitesse = \frac{distance}{temps}">

On aura obtiendra donc ici la vitesse moyenne du trajet en km/seconde, ce qui est une unité peu parlante mais qui permettra de comparer les trajets entre eux. On pourra par exemple voir qui sont les taxis ayant une vitesse moyenne plus rapide que les autres. Si nécessaire on peut ensuite adapter l'unité. Il faudra pour ça un avis métier et une connaissance plus précise de l'utilité de cette métrique.

### MapReduce vs groupByKey

Pour calculer le nombre de trajet en fonction du jour de la semaine, le nombre de trajet par tranche horraire et le nombre de km par jour de la semaine, j'ai utilisé le MapReduce.
Pourquoi utiliser le MapReduce avec le `reduceByKey` et non pas le `groupByKey`.
Lorsque le montant de données est élévé, `groupByKey` est moins performant que `reduceByKey`. Voyons pourquoi.


Lorsqu'on utilise les RDD, chaque morceau de rdd est appelé partition. Chaque partition appartient à un executeur.
`groupByKey` va chercher à regrouper les clés identiques dans la même partitition pour ensuite faire le traitement.
Il y a deux conséquences à cela :

- Cela produit un shuffle assez important des données car il doit le faire pour chaque donnée de chaque partition du RDD. Donc, si les partitions ne sont pas sur une même machine, cela provoque un gros trafic réseau.
- Si dans notre exemple les trajets avaient lieu à 99% le samedi, groupByKey rassemblera toutes les données ayant comme clé "samedi" dans une seule machine, ce qui peut causer des problèmes de mémoire.

Pour `reduceByKey`cela se passe autrement. Il y a d'abord un prétraitement dans chaque partition. Ensuite les nouvelles clé/valeur sont déplacées dans les partitions selon leur clé pour avoir leur traitement final.
La quantité de données est donc moins élevée, réduisant ainsi le temps de traitement.



## Lancement d'un job 

### list des jobs
Une fois le projet téléchargé on peut lancer 5 jobs : 
- trip_speed 
- trip_per_day_of_week
- trip_per_time_slice
- km_per_day_of_week
- compute_all_jobs

### Comment lancer un job 

Pour lancer un job il faut executer les commandes suivantes (à partir de la racine du dossier) : 
```shell
make build
cd dist && spark-submit --py-files jobs.zip main.py --<nom du job>
```

Les fichiers créés par le job sont sauvegardés dans le dossier /dist/outputs

Pour supprimer le dossier dist il suffit de faire la commande suivante 

```shell
make clean
```

Cette structure permet le packaging de nos jobs et de l'importer simplement.
Chaque job peut ainsi être modifié indépendamment des autres.

De plus grâce au Makefile il est très simple de supprimer et reconstruire l'environement de production. 
