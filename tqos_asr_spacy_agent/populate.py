from pykafka import KafkaClient
from pykafka.common import CompressionType
import simplejson as json

paragraphs  = [
    "CO2 causes climate change.",
    "Climate change is caused by CO2.",
    "Some scientists believe that carbon dioxide causes climate change.",
    "PO augments the contribution of GNG to EGP.",
    "The pandemic of obesity, type 2 diabetes mellitus (T2DM) and nonalcoholic fatty liver disease (NAFLD) has frequently been associated with dietary intake of saturated fats (1) and specifically with dietary palm oil (PO) (2).",
    "Diabetes mellitus type 2 is a long term metabolic disorder that is characterized by high blood sugar, insulin resistance, and relative lack of insulin. Common symptoms include increased thirst, frequent urination, and unexplained weight loss. Symptoms may also include increased hunger, feeling tired, and sores that do not heal. Often symptoms come on slowly. Long-term complications from high blood sugar include heart disease, strokes, diabetic retinopathy which can result in blindness, kidney failure, and poor blood flow in the limbs which may lead to amputations. The sudden onset of hyperosmolar hyperglycemic state may occur; however, ketoacidosis is uncommon.",
    "With the rising prevalence of obesity and related health problems increases, there is increased interest in the gastrointestinal system as a possible target for pharmacological or food-based approaches to weight management. Recent studies have shown that under normal physiological situations undigested nutrients can reach the ileum, and induce activation of the so-called \"ileal brake\", a combination of effects influencing digestive process and ingestive behaviour. The relevance of the ileal brake as a potential target for weight management is based on several findings: First, activation of the ileal brake has been shown to reduce food intake and increase satiety levels. Second, surgical procedures that increase exposure of the ileum to nutrients produce weight loss and improved glycaemic control. Third, the appetite-reducing effect of chronic ileal brake activation appears to be maintained over time. Together, this evidence suggests that activation of the ileal brake is an excellent long-term target to achieve sustainable reductions in food intake. This review addresses the role of the ileal brake in gut function, and considers the possible involvement of several peptide hormone mediators. Attention is given to the ability of macronutrients to activate the ileal brake, and particularly variation attributable to the physicochemical properties of fats. The emphasis is on implications of ileal brake stimulation on food intake and satiety, accompanied by evidence of effects on glycaemic control and weight loss.",
    "Inflammatory bowel diseases involve the dynamic interaction of host genetics, the microbiome and inflammatory responses. Here we found lower expression of NLRP12 (which encodes a negative regulator of innate immunity) in human ulcerative colitis, by comparing monozygotic twins and other patient cohorts. In parallel, Nlrp12 deficiency in mice caused increased basal colonic inflammation, which led to a less-diverse microbiome and loss of protective gut commensal strains (of the family Lachnospiraceae) and a greater abundance of colitogenic strains (of the family Erysipelotrichaceae). Dysbiosis and susceptibility to colitis associated with Nlrp12 deficency were reversed equally by treatment with antibodies targeting inflammatory cytokines and by the administration of beneficial commensal Lachnospiraceae isolates. Fecal transplants from mice reared in specific-pathogen-free conditions into germ-free Nlrp12-deficient mice showed that NLRP12 and the microbiome each contributed to immunological signaling that culminated in colon inflammation. These findings reveal a feed-forward loop in which NLRP12 promotes specific commensals that can reverse gut inflammation, while cytokine blockade during NLRP12 deficiency can reverse dysbiosis.",
    "Celiac disease is an autoimmune disorder in which the immune system mistakenly attacks the lining of the small intestine after someone who is genetically susceptible to the disorder ingests gluten from wheat, rye, or barley. This leads to a range of gastrointestinal symptoms, including abdominal pain, diarrhea, and bloating.",
    "Daily intake of metabolisable energy and crude protein were: 5.8 MJ, 109 g; 9.0 MJ, 84 g; 9.5 MJ, 84 g and 8.5 MJ, 88 g in H, HWC, HGC and HEC, respectively.",
    "Genome-wide association studies have identified 21 susceptibility loci associated with melanoma. These loci implicate genes affecting pigmentation, nevus count, telomere maintenance, and DNA repair in melanoma risk. Here, we report the results of a two-stage genome-wide association study of melanoma. The stage 1 discovery phase consisted of 4,842 self-reported melanoma cases and 286,565 controls of European ancestry from the 23andMe research cohort and the stage 2 replication phase consisted of 1,804 melanoma cases and 1,026 controls from the University of Texas M.D. Anderson Cancer Center. We performed a combined meta-analysis totaling 6,628 melanoma cases and 287,591 controls. Our study replicates 20 of 21 previously known melanoma-loci and confirms the association of the telomerase reverse transcriptase, TERT, with melanoma susceptibility at genome-wide significance. In addition, we uncover a novel polymorphism, rs187843643 (OR = 1.96; 95% CI = [1.54, 2.48]; P = 3.53 x 10–8), associated with melanoma. The SNP rs187842643 lies within a noncoding RNA 177kb downstream of BASP1 (brain associated protein-1). We find that BASP1 expression is suppressed in melanoma as compared with benign nevi, providing additional evidence for a putative role in melanoma pathogenesis.",
    "The metabolic basis of Alzheimer disease (AD) pathology and expression of AD symptoms is poorly understood. Omega-3 and -6 fatty acids have previously been linked to both protective and pathogenic effects in AD. However, to date little is known about how the abundance of these species is affected by differing levels of disease pathology in the brain.",
    "Human Hp is a plasma α2-glycoprotein characterized by 3 common phenotypes (Hp 1-1, Hp 2-1 and Hp 2-2). Its free hemoglobin (Hb)-binding capacity prevents Hb-driven oxidative damage. When the antioxidant capacity of Hp is insufficient, its role is taken over by hemopexin (heme-binding protein) and by vitamin C (free radical scavenger) [13]. Hp phenotypes show important structural and functional differences, which offer a plausible explanation how during the course of human history, some populations characterized by a high Hp 1 allele frequency have been able to survive on a vitamin C poor diet [6]. In comparison with the other Hp phenotypes, Hp 2-2 is associated with a better iron conservation (characterized by a higher serum iron concentration, higher serum ferritin concentrations and increased transferrin saturation levels) which may act synergistic in presence of hemochromatosis [14,15]. However, the Hp 2-2 phenotype has also a major impact on vitamin C stability in vivo [13]. Hp 2-2 subjects are less efficient in removing free Hb from plasma, which may favour an iron-mediated vitamin C depletion [13,16]. Iron retention in scurvy-prone Hp 2-2 individuals results in iron-driven oxidative stress, which is reflected by lower serum vitamin C concentrations in healthy Hp 2-2 subjects [3].",
    "We subsequently performed a genome-wide association study and identified the TMEM106B and GRN gene loci, previously associated with frontotemporal dementia, as determinants of Δ-aging in the cerebral cortex with genome-wide significance. TMEM106B risk variants are associated with inflammation, neuronal loss, and cognitive deficits, even in the absence of known brain disease, and their impact is highly selective for the frontal cerebral cortex of older individuals (>65 years)."
]

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Add paragraphs to kafka')
    parser.add_argument('--docid', '-d', type=str,
                        default="0",
                        help='document id')
    parser.add_argument('--topic', '-t', type=str,
                        default='paragraphs',
                        help='kafka topic')
    parser.add_argument('--compression', '-c', type=str,
                        default='NONE',
                        help='compression method (GZIP, SNAPPY, LZ4, NONE)')
    parser.add_argument('--zookeeper', '-z', type=str,
                        default='127.0.0.1:2181',
                        help='zookeeper host address and port')

    args = parser.parse_args()
    client = KafkaClient(zookeeper_hosts=args.zookeeper)
    topic = client.topics['paragraphs']
    compression = getattr(CompressionType, args.compression) if args.compression else None
    # maybe try with async?
    with topic.get_sync_producer(compression=compression) as producer:
        i=len(paragraphs)
        data = {
            "para_info": {
                "doc_id": args.docid,
                "para_id": i
            },
            "text": "We have found methane in artic ocean permafrost"
        }
        producer.produce(json.dumps(data).encode('utf-8'),
                partition_key=b"%s_%d" % (args.docid.encode('ascii'), i))
 #        for i, s in enumerate(paragraphs):
 #            data = {"docId": args.docid, "paraId": i, "text": s}
 #            producer.produce(json.dumps(data).encode('utf-8'),
 #                partition_key=b"%s_%d" % (args.docid.encode('ascii'), i))
 # 