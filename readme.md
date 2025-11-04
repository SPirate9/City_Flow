# Guide d'utilisation et de déploiement AWS

## Projet : **Analyse des impacts des travaux urbains sur les flux de circulation à Paris**

### Objectif
Exploiter et croiser les données de **capteurs de trafic routier** et les **données ouvertes des chantiers urbains** afin d'aider les acteurs publics et privés à prendre des **décisions éclairées**.

---

## 1. Architecture générale 🏗️

### Collecte de données
* **Sources :**
    * **Trafic routier** (Fichiers CSV de base)
    * **Données ouvertes des chantiers de la Ville de Paris** (CSV via Open Data API)
* **Services AWS utilisés :**
    * **Lambda** : Extraction, transformation et ingestion automatisée des données depuis les APIs ou S3.
    * **EC2** : Traitements lourds, scripts de collecte ou jobs batch pour capteurs.
    * **EventBridge** : Planification et orchestration des flux ETL.
    * **CloudWatch** : Surveillance, métriques et logs.

### Stockage
* **S3** : Stockage des données brutes, nettoyées et des exports (par préfixes : `raw/`, `clean/`, `reports/`).
* **DynamoDB** : Stockage des données structurées (résultats rapides pour l'API).
* **Versioning** : Versioning et Snapshots activé sur les buckets S3.

### API et accès utilisateur
* **API Gateway** : Exposition REST (ou HTTP) des endpoints publics/privés.
* **Lambdas frontales** Pour répondre aux endpoints (lecture depuis DynamoDB / S3).
* **Postman** : Tests et collection d'API pour équipes/clients.

### Sécurité et gestion des accès
* **IAM** : Rôles et policies appliquant le principe du **moindre privilège**.
* **Token et limite de requête** : Token et limite de requêtes associées.

### Monitoring, observabilité & résilience
* **CloudWatch Logs & Metrics** : logs centralisés, métriques custom (latence, taux d'erreur).
* **Dashboards CloudWatch** : vues opérationnelles.
* **DLQ (SQS)** pour Lambdas (et retry policies).
* **Auto Scaling** pour EC2 si nécessaire.



---

## 2. Déploiement — étape par étape ⚙️

### 2.1. Préparer les comptes et permissions IAM
1.  Créer un **utilisateur IAM** ou rôle service pour le déploiement.
2.  Appliquer le principe du moindre privilège : policies séparées pour les actions S3, Lambda, DynamoDB, CloudWatch, EventBridge.
3.  Configurer l'**AWS CLI** / `profiled credentials` pour les pipelines CI.

### 2.2. Collecte des données
* **Données capteurs de trafic (batch)** :
    * EC2 exécute les scripts de récupération et pré-traitement.
    * Standardiser / normaliser en **CSV/Parquet**.
    * Pousser les résultats dans : `s3://cityflow-raw-paris/batch/`.
* **Données chantiers (API)** :
    * Lambda planifiée via EventBridge appelle Open Data API (Ville de Paris).
    * Normalisation et sauvegarde : `s3://cityflow-raw-paris/api/`.
* **Automatisation** :
    * EventBridge : planifier les exécutions.
    * CloudWatch : alarme si l'exécution échoue plus de N fois.

### 2.3. Validation, transformation et croisement
* **Validation initiale** : vérifier schéma, colonnes obligatoires, types, valeurs manquantes et doublons.
    * Rejeter / mettre en quarantaine les données invalides et notifier l'équipe.
* **Transformation** : nettoyage, agrégation temporelle.
* **Stockage des résultats :**
    * Données analytiques nettoyées → **S3** (`clean/`).
    * Résumé / index → **DynamoDB** : table `Cityflow_metrics`, `Traffic` et `WorksImpact`


### 2.4. Exposition via API

GET /chantiers 

GET /traffic-summary

GET /traffic-metrics

GET /top-congested


### 2.5. Sécurité et bonnes pratiques
* **Ne jamais stocker de clés IAM dans le code.**
* Utiliser **roles IAM** (Lambda/EC2).
* Révision régulière des **policies IAM** et rotation des credentials.

### 2.6. Maintenance & résilience opérationnelle
* Nettoyage automatisé (Lambda) des données obsolètes.
* **DLQ + retries** pour les Lambdas.
* **Backups** (ex : sauvegarde ponctuelle de DynamoDB si nécessaire).

---

## 3. Guide utilisateur 🧑‍💻

### Accès aux données via API
* Utiliser **Postman** ou un client HTTP.

### Visualisation
* Export CSV depuis S3 ou requêtes via Athena.
* Dashboards possibles : **QuickSight, Power BI**
* Business Values
Carte des chantiers / Où se concentrent les travaux / Identifier les zones à risque de congestion
Carte du trafic / Où ça coince actuellement / Prioriser les axes à surveiller
Bar chart par impact / Quels types de travaux gênent le plus / Aider à catégoriser et anticiper les perturbations
Courbe temporelle du trafic / Quand le trafic se dégrade / Soutenir des décisions de planification (calendrier, horaires, phasage)

### Automatisation
* Flux ETL automatisés via **EventBridge**.
* **Notifications (SNS / Slack)** en cas d’échec.

---

## 4. Évolutions possibles ✨
* **Predictions** : modèles ML pour simuler impacts futurs (SageMaker).
* **Streaming** : si capteurs passent à du temps réel → ingestion via Kinesis.
* **Multi-région** : réplication S3 / haute disponibilité.
* **Dashboard temps réel** : Websocket + API Gateway + Lambdas.

---

Fait par Saad Shahzad 
         Thomas Yu
         Noam Boulze
