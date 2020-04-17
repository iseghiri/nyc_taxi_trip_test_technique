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
