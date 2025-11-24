from backend.agents.simple.formatter import FormatterAgent


def main():
    # Configuration de test du FormatterAgent
    config = {
        "strip_strings": True,
        "empty_to_none": True,
        "date_fields": ["invoice_date"],
        "input_date_formats": ["%d/%m/%Y", "%Y-%m-%d"],
        "output_date_format": "%Y-%m-%d",
        "numeric_fields": ["ht", "tva", "ttc"],
        "decimals": 2,
        "as_string": True,
    }

    # Créer l'agent
    agent = FormatterAgent(config=config)
    agent.validate()

    # Input de test
    inputs = {
        "invoice_date": "31/01/2023",
        "ht": "100",
        "tva": "20,5",
        "ttc": "120.5",
        "comment": "  bonjour  ",
        "empty_field": "   "
    }

    # Exécution de l'agent
    result = agent.run(inputs)

    # Affichage du résultat final
    print("Résultat FormatterAgent :", result)


if __name__ == "__main__":
    main()
