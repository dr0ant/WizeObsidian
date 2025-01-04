import pandas as pd
import re

# Example markdown content (replace this with the content of your markdown file)
markdown_content = """1. Informations générales
Nom de la civilisation : Les Orcs
Population totale : Majoritairement concentrée en La Kramargue, population répartie en tribus nomades.
Localisation géographique :
Continent : La Kramargue.
Climat : Montagneux/plaine, varié selon les saisons.
Topographie : Grandes plaines et petites montagnes, adaptées au mode de vie nomade.
Capitale :Rariss centre culturel de réunion des tribus Orcs malgré un mode de vie nomade.
Autres villes importantes : Non applicable en raison de leur organisation en tribus.
2. Organisation sociale
Système politique : Organisation tribale, dirigée par des chefs choisis selon leur force et leur instinct.
Hiérarchie sociale : Basée sur la force, l’instinct et les compétences (notamment dans l’élevage et le combat), les tribus les plus puissantes ont souvent le plus grand troupeau.
Rôles genrés et familiaux : Les hommes à la chasse et les femmes a la cueillette et la cuisine. A priori la société Orc semble très très genré mais les femmes sont totalement capable de prendre les rôles des hommes et inversement. Une forme d’harmonie règne
Valeurs fondamentales : Solidarité très forte, partage, respect de la faune.
3. Économie
Ressources principales : Cuir, os, bêtes élevées (troupeaux), objets d’artisanat liés à la faune.
Produits artisanaux et industries : Articles en cuir, armes en os de mammouth, élevage de bêtes, et viandes.
Système monétaire : échanges entre tribus, échanges avec les Nains pour le métal en contrepartie de cuir et de bêtes. Utilisation forte du Le Kyrr pour les échanges à l'international et avec les Les Nains , Les Hommes et les Les Phoséidiens de l'alliance Yamé (Faction Yamé)
Commerce : Étroitement lié aux Nains de Kramargue, coopération entre artisanat orc et nain. Les montures élevée par les Orcs sont reconnues dans le monde entier.
Travail : Éleveurs, chasseurs, artisans spécialisés dans le travail des matériaux naturels.
"""

# Define a regex pattern to capture section titles and their content
pattern = r"""(?P<section_number>\d+)\. (?P<section_title>[\w\s]+)\n(?P<section_content>(?:.+\n?)+?)(?=\n\d+\. |$)"""

# Find all matches in the markdown content
matches = re.finditer(pattern, markdown_content, re.MULTILINE)

# Extract sections into a list of dictionaries
sections = []
for match in matches:
    section_number = match.group("section_number")
    section_title = match.group("section_title").strip()
    section_content = match.group("section_content").strip()
    sections.append({
        "Section Number": section_number,
        "Section Title": section_title,
        "Content": section_content
    })

# Convert the list of dictionaries to a pandas DataFrame
df = pd.DataFrame(sections)

# Save the DataFrame to a CSV file
df.to_csv("sections.csv", index=False)

# Print the DataFrame
print(df)
