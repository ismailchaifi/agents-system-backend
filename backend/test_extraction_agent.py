from backend.agents.llm.extraction import ExtractionAgent


def main():
    config = {
        "fields": ["supplier", "invoice_date", "ht", "tva", "ttc"],
        "model": "gpt-4o-mini",
        "instructions": "Le champ 'invoice_date' doit être au format YYYY-MM-DD si possible.",
    }

    agent = ExtractionAgent(config=config)
    agent.validate()

    text = """
    FACTURE
    Fournisseur : SuperMarché Alpha
    Date : 31/01/2023
    Total HT : 100 EUR
    TVA : 20 EUR
    Total TTC : 120 EUR
    """

    result = agent.run(text)
    print("Résultat ExtractionAgent :", result)


if __name__ == "__main__":
    main()
