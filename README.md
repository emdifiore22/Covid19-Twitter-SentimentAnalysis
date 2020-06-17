# Covid19 Twitter SentimentAnalysis
The aim is to define a sentiment analysis algorithm using twitter dataset to detect the reactions of italian (european) people during recent lockdown.

<body>
  <div tabindex="-1" id="notebook" class="border-box-sizing">
    <div class="container" id="notebook-container">

<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<p><h1>Tesina di Big Data and Business Intelligence - Sentiment Analysis</h1></p>
<h3>Autori: Emanuele Di Fiore, Roberto Di Luca</h3><div style="text-align: justify"> 
<br>Nella presente tesina è stata svolta una Sentiment Analysis su un dataset di Tweet estratti dal social network Twitter per valutare le reazioni degli utenti durante il recente lockdown causato dalla diffusione del virus Covid-19. Le tecnologie utilizzate sono state:
</div>
</div>
</div>
</div>
<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<ul>
<li>Apache Spark 2.4.5 (tramite la libreria pyspark) come ambiente per il preprocessing dei tweet, per la loro etichettatura, per l'addestramento e la valutazione di un modello di Machine Learning (ML);</li>
<li>MongoDB come database NoSQL per lo storage dei tweet;</li>
<li>Python per l'estrazione dei tweet tramite il tool Twarc.</li>
</ul>
<p>Il lavoro è strutturato come segue:</p>
<ul>
<li><a href="#1">Cenni alla teoria della Sentiment Analysis, estrazione dei tweet e introduzione al loro formato</a></li>
<li><a href="#2">Etichettatura del dataset usando la libreria nltk (modello Vader)</a>;</li>
<li><a href="#3">Addestramento di un modello di ML sul dataset etichettato e calcolo dell'accuracy</a>;</li>
<li><a href="#4">SentiWordNet per l'etichettatura del dataset</a>;</li>
<li><a href="#5">Analisi delle performance di Spark</a>.</li>
</ul>
<p>Inoltre è presente un'<a href="#7">appendice</a> riguardante l'installazione in locale di Spark e MongoDB.
<br>Infine, una sezione per i <a href="#8">riferimenti</a> utilizzati nella trattazione.</p>
<p>Tutto il codice sviluppato è presente su Github al seguente <a href="https://github.com/emdifiore22/Covid19-Twitter-SentimentAnalysis">link</a>.</p>

</div>
</div>
</div>
<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<p><a id='1'>
    <h3>Sentiment Analysis e formato dei tweet</h3>
</a></p>
<div style="text-align: justify">
<br>Ogni giorno, grazie ai social network, ai blog o ad altri sistemi di raccomandazione, vengono scambiati milioni di messaggi su Internet. Tali messaggi possono essere suddivisi in due principali categorie: fatti e opinioni. I fatti sono affermazioni oggettive, mentre le opinioni riflettono un sentimento di un utente rispetto a un utente, altre persone o eventi e sono molto importanti quanche c'è la necessità di prendere delle decisioni. L'espressione <b>Sentiment Analysis</b> (anche nota come Opinion Mining) fa riferimento all'uso di tecniche di Natural Language Processing (NLP), Text Analysis e Linguistica Computazionale per identificare ed estrarre informazioni soggettive in documenti, commenti e post [1].
<br>Un esempio di applicazione si ha nelle aziende che forniscono prodotti o servizi e che quindi sono interessate a conoscere i commenti o le opinioni dei loro clienti per garantire loro una qualità sempre più elevata.
<br>Durante il recente periodo di lockdown che abbiamo vissuto a causa del virus Covid-19, molte persone hanno usato i social network, Twitter in particolare, per esprimere i loro stati d'animo. In questa tesina, è stata usata una parte del dataset GeoCoV19 [2]. I tweet scaricati sono stati memorizzati su MongoDB e preprocessati e analizzati tramite Spark al fine di conoscere quali sono stati i sentimenti predominanti.
</div>
</div>
</div>
</div>
<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<p><h5>Perché MongoDB?</h5></p>
<div style="text-align: justify">
<br>Le tecnologie NoSQL sono particolarmente note per la loro caratteristica di essere "schema-less", ovvero i dati in essi memorizzati non devono necessariamente sottostare a uno schema prefissato così come nelle soluzioni relazionali. Tale caratteristica si presta molto bene al caso in esame, in cui i singoli tweet (la cui struttura è spiegata nel seguito) non rispettano rigorosamente una struttura. Ad esempio, se un tweet è originale (ovvero non ne ricondivide un altro) non presenta alcuni campi, tra cui il campo "retweeted_status".
<br> In particolare, tra le varie soluzioni non relazionali, è stato scelto <b>MongoDB</b> per il suo orientamento ai documenti. Infatti, memorizza i dati in un formato JSON-like (BSON, Binary JSON), che è un "cugino" del formato JSON restituito dal tool utilizzato per estrarre i tweet. Questo ha reso molto rapida l'importazione dei dati nel DB.
<br> Nella scelta di un DB NoSQL pesa anche il tipo di operazioni che si intende effettuare sui dati. Nel nostro caso, non è stato necessario analizzare eventuali relazioni tra i tweet, ma solo dei filtraggi sulla base dei valori per alcuni campi. Ciò ci ha portato a dire che una soluzione diversa da quella documentale, ad esempio una orientata ai grafi (Neo4j), non è la più indicata.
</div>
</div>
</div>
</div>
<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<p><h5>Estrazione dei Tweet</h5>
<br>Sebbene i termini d'uso delle API di Twitter sconsiglino la condivisione via web dei dati raccolti, consentono quella di file contenenti gli id dei tweet. A questo punto, con un processo noto come "Hydration" è possibile ricavare l'intera struttura dati del tweet tramite il suo id.
<br>Il processo di Hydration è molto semplice: data una collezione di identificativi (id) è possibile utilizzare <b>Twarc</b>, un tool, disponibile anche come libreria Python, che permette di scaricare tweet rappresentati in formato JSON. Per poter effettuare il download dei tweet è necessario creare un’app con un account developer di Twitter, ottenere le chiavi Consumer API e i token di accesso e utilizzare tali informazioni per abilitare Twarc al download. In particolare, tale abilitazione può avvenire in due modi:
&lt;/div&gt;</p>

</div>
</div>
</div>
<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<ul>
<li>Utilizzando il tool a linea di comando: lanciare twarc configure e seguire tutte le indicazioni per la registrazione delle chiavi e dei token</li>
<li>In Python, utilizzare il costruttore:</li>
</ul>

</div>
</div>
</div>
<div class="cell border-box-sizing code_cell rendered">
<div class="input">
<div class="prompt input_prompt">In&nbsp;[&nbsp;]:</div>
<div class="inner_cell">
    <div class="input_area">
<div class=" highlight hl-ipython3"><pre><span></span><span class="n">t</span> <span class="o">=</span> <span class="n">Twarc</span><span class="p">(</span><span class="n">consumer_key</span><span class="p">,</span> <span class="n">consumer_secret</span><span class="p">,</span> <span class="n">access_token</span><span class="p">,</span> <span class="n">access_token_secret</span><span class="p">)</span>
</pre></div>

    </div>
</div>
</div>

</div>
<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<div style="text-align: justify">
<br>Per poter estrarre i tweet è stato utilizzato lo script TwarcTwitterExtraction.py, disponibile nel repository GitHub del progetto. Tale script utilizza in input una lista di identificativi e genera in output un file in formato JSON Lines contenente i JSON dei tweet. Per tale elaborato sono stati gli identificativi associati alle date:
</div>
</div>
</div>
</div>
<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<ul>
<li>13/02/2020</li>
<li>16/03/2020</li>
<li>29/03/2020</li>
<li>30/03/2020</li>
<li>08/04/2020</li>
<li>29/04/2020</li>
</ul>
<p>A causa dell’elevata quantità di id presenti in questi file, essi sono stati processati soltanto in parte, arrivando a raccogliere 113965 tweet.</p>

</div>
</div>
</div>
<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<p><h5>Struttura di un tweet</h5>
<br>In generale, un utente può postare un tweet in due modi:</p>
<ul>
<li>Scrivendo un contenuto originale;</li>
<li>Condividendo un tweet di un altro utente.</li>
</ul>
<p>Dalla struttura JSON di un tweet è quindi possibile capirne la tipologia, il testo associato, informazioni sull’utente che l’ha postato, la data, le informazioni di geolocalizzazione, etc. Per lo svolgimento dell’elaborato sono stati fondamentali i seguenti campi o strutture:</p>
<ul>
<li><b>full_text</b>: contenente il testo completo del tweet;</li>
<li><b>lang</b>: contenente la lingua con cui è stato scritto quel tweet;</li>
<li><b>retweeted_status</b>: in caso di retweet, rappresenta una struttura innestata contenente tutte le informazioni del tweet che è stato ricondiviso (autore, testo del post, etc.), altrimenti il campo non è presente.</li>
</ul>
<p>Di seguito è mostrata la struttura completa di un tweet con e senza retweeted_status.
<img src="images/tweet_json.jpg" alt="JSON dei tweet"></p>
<p><h5>Filtraggio</h5>
<br>Una volta memorizzati tutti i tweet in una collection MongoDB, si è deciso di utilizzare per la Sentiment Analysis tutti i tweet in lingua inglese (campo lang uguale a ‘en’) e di selezionare il campo full_text per i tweet originali ed il campo retweeted_status.full_text per i tweet frutto di ricondivisione. Tale filtraggio si può facilmente effettuare tramite delle query ad hoc sul database. Le possibilità sono due:</p>
<ul>
<li>Utilizzare il tool <b>MongoDB Compass</b> per effettuare le query utilizzando l’interfaccia grafica e poi esportare i risultati in formato JSON (da caricare successivamente in ambiente Spark);</li>
<li>Utilizzare direttamente <b>MongoDB Spark Connector</b>.</li>
</ul>
<p>Nel primo caso abbiamo:
<img src="./images/query_no_retweet.jpg" alt="MongoDB Compass">
<img src="./images/query_retweet.jpg" alt="MongoDB Compass"></p>
<p>Nel secondo caso abbiamo il codice seguente:</p>

</div>
</div>
</div>
<div class="cell border-box-sizing code_cell rendered">
<div class="input">
<div class="prompt input_prompt">In&nbsp;[&nbsp;]:</div>
<div class="inner_cell">
    <div class="input_area">
<div class=" highlight hl-ipython3"><pre><span></span><span class="n">pipeline_noRetweet</span> <span class="o">=</span> <span class="s2">&quot;[</span><span class="se">\</span>
<span class="s2">    {</span><span class="se">\</span>
<span class="s2">        &#39;$match&#39;: {</span><span class="se">\</span>
<span class="s2">            &#39;lang&#39;: &#39;en&#39;,</span><span class="se">\</span>
<span class="s2">            &#39;retweeted_status&#39;:null</span><span class="se">\</span>
<span class="s2">        }</span><span class="se">\</span>
<span class="s2">    },{</span><span class="se">\</span>
<span class="s2">        &#39;$project&#39;: {</span><span class="se">\</span>
<span class="s2">            &#39;id_str&#39;: 1</span><span class="se">\</span>
<span class="s2">            &#39;created_at&#39;: 1</span><span class="se">\</span>
<span class="s2">            &#39;full_text&#39;: 1</span><span class="se">\</span>
<span class="s2">        },</span><span class="se">\</span>
<span class="s2">    }</span><span class="se">\</span>
<span class="s2">]&quot;</span>

<span class="n">pipeline_Retweet</span> <span class="o">=</span> <span class="s2">&quot;[</span><span class="se">\</span>
<span class="s2">    {</span><span class="se">\</span>
<span class="s2">        &#39;$match&#39;: {</span><span class="se">\</span>
<span class="s2">            &#39;lang&#39;: &#39;en&#39;</span><span class="se">\</span>
<span class="s2">            &#39;retweeted_status&#39;:{$ne: null}</span><span class="se">\</span>
<span class="s2">            &#39;retweeted_status.full_text&#39;:&#39;en&#39;</span><span class="se">\</span>
<span class="s2">        }</span><span class="se">\</span>
<span class="s2">    },{</span><span class="se">\</span>
<span class="s2">        &#39;$project&#39;: {</span><span class="se">\</span>
<span class="s2">            &#39;id_str&#39;: 1</span><span class="se">\</span>
<span class="s2">            &#39;created_at&#39;: 1</span><span class="se">\</span>
<span class="s2">            &#39;retweeted_status.full_text&#39;: 1</span><span class="se">\</span>
<span class="s2">        },</span><span class="se">\</span>
<span class="s2">    }</span><span class="se">\</span>
<span class="s2">]&quot;</span>

<span class="n">df_ENGNoRetweet</span> <span class="o">=</span> <span class="n">spark</span><span class="o">.</span><span class="n">read</span><span class="o">.</span><span class="n">format</span><span class="p">(</span><span class="s2">&quot;com.mongodb.spark.sql.DefaultSource&quot;</span><span class="p">)</span><span class="o">.</span><span class="n">option</span><span class="p">(</span><span class="s2">&quot;pipeline&quot;</span><span class="p">,</span> <span class="n">pipeline_noRetweet</span><span class="p">)</span><span class="o">.</span><span class="n">load</span><span class="p">()</span>
<span class="n">df_ENGRetweet</span> <span class="o">=</span> <span class="n">spark</span><span class="o">.</span><span class="n">read</span><span class="o">.</span><span class="n">format</span><span class="p">(</span><span class="s2">&quot;com.mongodb.spark.sql.DefaultSource&quot;</span><span class="p">)</span><span class="o">.</span><span class="n">option</span><span class="p">(</span><span class="s2">&quot;pipeline&quot;</span><span class="p">,</span> <span class="n">pipeline_Retweet</span><span class="p">)</span><span class="o">.</span><span class="n">load</span><span class="p">()</span>

<span class="n">df_ENGNoRetweet</span><span class="o">.</span><span class="n">printSchema</span><span class="p">()</span>
<span class="n">df_ENGRetweet</span><span class="o">.</span><span class="n">printSchema</span><span class="p">()</span>
</pre></div>

    </div>
</div>
</div>

</div>
<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<div style="text-align: justify">
In entrambi i casi l’idea è stata quella di selezionare, in una prima query, tutti i tweet che non presentano il campo retweeted_status e ricavare da essi il campo full_text, mentre in una seconda query il campo full_text è ricavato solo da quei tweet che presentano il campo retweeted_status. 
<br>In particolare, nel secondo caso (tweet frutto di ricondivisione) il campo full_text è identico al campo retweeted_status.full_text a meno dell’aggiunta di alcuni caratteri. Facciamo un esempio per chiarire la situazione. Supponiamo che Bob abbia ricondiviso il tweet “Spark è potentissimo!” di Alice, il tweet di Bob presenta nella propria struttura JSON:
</div><ul>
<li><b>full_text</b>: “RT @alice: Spark è potentissimo!”</li>
<li><b>retweeted_status.full_text</b>: “Spark è potentissimo!”</li>
</ul>
<div style="text-align: justify">
Come si vede, il contenuto utile del tweet è lo stesso, si è deciso di considerare solo il secondo.
Poiché, in generale, può capitare che più utenti ricondividano lo stesso tweet, potrebbero esserci dei testi duplicati. Per rimuoverli è stato utilizzata una distinct() sul DataFrame contenente i tweet.
</div>
</div>
</div>
</div>
<div class="cell border-box-sizing code_cell rendered">
<div class="input">
<div class="prompt input_prompt">In&nbsp;[&nbsp;]:</div>
<div class="inner_cell">
    <div class="input_area">
<div class=" highlight hl-ipython3"><pre><span></span><span class="n">df_Tweets</span> <span class="o">=</span> <span class="n">df_ENGRetweet</span>\
    <span class="o">.</span><span class="n">selectExpr</span><span class="p">(</span><span class="s2">&quot;id_str&quot;</span><span class="p">,</span> <span class="s2">&quot;retweeted_status.full_text as full_text&quot;</span><span class="p">)</span>\
    <span class="o">.</span><span class="n">union</span><span class="p">(</span><span class="n">df_ENGNoRetweet</span><span class="o">.</span><span class="n">select</span><span class="p">(</span><span class="s2">&quot;id_str&quot;</span><span class="p">,</span> <span class="s2">&quot;full_text&quot;</span><span class="p">))</span>


<span class="n">df_Tweets</span> <span class="o">=</span> <span class="n">df_Tweets</span><span class="o">.</span><span class="n">select</span><span class="p">(</span><span class="s2">&quot;full_text&quot;</span><span class="p">)</span><span class="o">.</span><span class="n">distinct</span><span class="p">()</span>
<span class="n">df_Tweets</span><span class="o">.</span><span class="n">count</span><span class="p">()</span>
</pre></div>

    </div>
</div>
</div>

</div>
<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<p>A valle del filtraggio, i tweet selezionati per la Sentiment Analysis sono 43710.</p>

</div>
</div>
</div>
<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<p><a id='2'>
    <h3>Etichettatura tramite VADER</h3>
</a></p>
<div style="text-align: justify">
<br>Uno dei problemi principali nell'addestramento di modelli di ML per la sentiment analysis è la disponibilità di un dataset etichettato. Oltre ad essere un task time-consuming, l'etichettatura di un dataset del genere può essere anche complicata. Mentre per alcuni tweet è semplice estrarre la polarità del sentimento espresso (positiva, negativa, neutra), per altri può essere estremamente soggettiva. Di norma (e questo a prescindere dalla sentiment analysis), il labeling di un dataset è a cura di chi ha una profonda esperienza nel dominio che si sta trattando. Nella sentiment analysis, per quanto sopra riportato, questo è ancora più vero.
In questa tesina, assumiamo come metodo di etichettatura quello effettuato tramite un modello preaddestrato chiamato VADER. 
<br><b>VADER (Valence Aware Dictionary and sEntiment Reasoner) [3] </b> è un tool di Sentiment Analysis di tipo rule-based specificamente progettato per i sentimenti espressi nei social media. VADER usa un lessico semantico, ovvero una lista di parole etichettate in base al loro orientamento ad essere positive o negative.
<br>È stato osservato che VADER è molto performante quando si tratta di analizzare testi provenienti da social media, recensioni di film e di prodotti. Questo perché VADER non solo tratta i termini usuali del dizionario, ma anche espressioni tipiche del mondo della messaggistica istantanea come:
</div>
</div>
</div>
</div>
<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<ul>
<li>uso delle contrazioni linguistiche (ad es. "wasn't very good");</li>
<li>uso della punteggiatura per accentuare l'intensità di un sentimento (ad es. "Good!!!");</li>
<li>uso della forma delle parole per conferirle maggiore enfasi (ad es. le parole scritte in maiuscolo);</li>
<li>uso delle emoticon.</li>
</ul>
<div style="text-align: justify">
L'uso di VADER è estremamente semplice in python grazie al pacchetto nltk (Natural Language ToolKit). Di seguito è mostrata una porzione di codice che mostra come avviene l'etichettatura. Il metodo polarity_scores() permette di ricavare gli indici di polarità (positivo, negativo, neutro) per una determinata frase.
I positive, negative e neutral scores rappresentano la porzione di testo che ricade in tali categorie. Il compound score (valore compreso tra -1 e 1), invece, riassume in un unico valore la positività o la negatività di un testo. Se 1, il testo è totalmente positivo, -1 altrimenti.
</div>
</div>
</div>
</div>
<div class="cell border-box-sizing code_cell rendered">
<div class="input">
<div class="prompt input_prompt">In&nbsp;[&nbsp;]:</div>
<div class="inner_cell">
    <div class="input_area">
<div class=" highlight hl-ipython3"><pre><span></span><span class="kn">import</span> <span class="nn">nltk</span>
<span class="kn">import</span> <span class="nn">sys</span>
<span class="kn">from</span> <span class="nn">nltk.sentiment.vader</span> <span class="kn">import</span> <span class="n">SentimentIntensityAnalyzer</span>
<span class="n">nltk</span><span class="o">.</span><span class="n">download</span><span class="p">(</span><span class="s2">&quot;vader_lexicon&quot;</span><span class="p">)</span>

<span class="k">def</span> <span class="nf">vaderSentimentAnalysis</span><span class="p">(</span><span class="n">data_str</span><span class="p">):</span>
    <span class="n">sid</span> <span class="o">=</span> <span class="n">SentimentIntensityAnalyzer</span><span class="p">()</span>
    <span class="n">ss</span> <span class="o">=</span> <span class="n">sid</span><span class="o">.</span><span class="n">polarity_scores</span><span class="p">(</span><span class="n">data_str</span><span class="p">)</span>
    <span class="k">return</span> <span class="n">ss</span>

<span class="n">vaderSentimentAnalysis_udf</span> <span class="o">=</span> <span class="n">udf</span><span class="p">(</span><span class="n">vaderSentimentAnalysis</span><span class="p">,</span> <span class="n">StringType</span><span class="p">())</span>

<span class="n">df_Tweets</span> <span class="o">=</span> <span class="n">df_Tweets</span><span class="o">.</span><span class="n">withColumn</span><span class="p">(</span><span class="s2">&quot;score&quot;</span><span class="p">,</span> <span class="n">vaderSentimentAnalysis_udf</span><span class="p">(</span><span class="n">df_Tweets</span><span class="p">[</span><span class="s1">&#39;full_text&#39;</span><span class="p">]))</span>
<span class="n">df_Tweets</span><span class="o">.</span><span class="n">show</span><span class="p">(</span><span class="n">truncate</span><span class="o">=</span><span class="mi">50</span><span class="p">)</span>
</pre></div>

    </div>
</div>
</div>

</div>
<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<p><a id='3'>
    <h3>Machine Learning</h3>
</a></p>
<div style="text-align: justify">
<br>In questa sezione, a partire dal dataset etichettato usando VADER, abbiamo addestrato un modello di ML di tipo <b>Naive Bayes</b>. La scelta di questo modello è motivata dal fatto che, sperimentalmente, tale genere di modello funziona particolarmente bene per scopi di Text Classification. Nonostante la sua semplicità, diversi sono i vantaggi derivanti dal suo uso: assenza di iperparametri da ottimizzare e velocità di addestramento rispetto ad altri modelli più complessi. 
<br>Di seguito è riportata la suddivisione del dataset (già opportunamente "ripulito" e suddiviso in token) in training e test set. 
</div>
</div>
</div>
</div>
<div class="cell border-box-sizing code_cell rendered">
<div class="input">
<div class="prompt input_prompt">In&nbsp;[&nbsp;]:</div>
<div class="inner_cell">
    <div class="input_area">
<div class=" highlight hl-ipython3"><pre><span></span><span class="c1"># Divisione Training e Test</span>
<span class="n">train</span><span class="p">,</span> <span class="n">test</span> <span class="o">=</span> <span class="n">df_TweetsCleaned</span><span class="o">.</span><span class="n">select</span><span class="p">(</span><span class="s2">&quot;words_nsw&quot;</span><span class="p">,</span> <span class="s2">&quot;label&quot;</span><span class="p">)</span><span class="o">.</span><span class="n">randomSplit</span><span class="p">([</span><span class="mf">0.75</span><span class="p">,</span><span class="mf">0.25</span><span class="p">],</span> <span class="n">seed</span><span class="o">=</span><span class="mi">2020</span><span class="p">)</span>
</pre></div>

    </div>
</div>
</div>

</div>
<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<div style="text-align: justify">
Le features estratte sono le classiche <b>TF-IDF (Term Frequency - Inverse Document Frequency)</b> [5].
<br> La <b>term frequency (TF)</b> di una parola è la frequenza di occorrenza di una parola in un documento (nel nostro caso un tweet). Ad esempio, se un documento D di 100 parole contiene la parola "cat" 12 volte, allora la TF della parola "cat" è 12/100 = 0.12.
<br> La <b>inverse document frequency (IDF)</b> di una parola è una misura che rispecchia l'importanza di un termine in una collezione documentale (nel nostro caso l'insieme dei tweet). Ad esempio, supponendo che una collezione documentale sia composta da 10 milioni di documenti e che la parola "cat" compaia solo in 300 mila documenti, la sua IDF è data dal log(10,000,000/300,000) = 1.52.
<br> In conclusione, la parola "cat" ha una TF-IDF per il documento D pari a TF*IDF = 0.12*1.52 = 1.82.
<br>In ML si è soliti eseguire una sequenza di algoritmi per processare e apprendere dai dati. Spark viene in aiuto all'esigenza di definire tali workflow fornendo l'astrazione di <b>Pipeline</b>. Una Pipeline consiste in una sequenza di stages (<b>Transformers</b>, che hanno come output dei DataFrame, ed <b>Estimators</b>, che hanno come output dei Transformers).
Nel codice riportato è stata definita una pipeline che, in sequenza, crea TF, IDF, e il modello Bayesiano. Infine è stato calcolato sia il resubstitution error, sia quello sul test set.
</div>
</div>
</div>
</div>
<div class="cell border-box-sizing code_cell rendered">
<div class="input">
<div class="prompt input_prompt">In&nbsp;[&nbsp;]:</div>
<div class="inner_cell">
    <div class="input_area">
<div class=" highlight hl-ipython3"><pre><span></span><span class="n">cv</span>  <span class="o">=</span> <span class="n">CountVectorizer</span><span class="p">(</span><span class="n">inputCol</span><span class="o">=</span><span class="s1">&#39;words_nsw&#39;</span><span class="p">,</span> <span class="n">outputCol</span><span class="o">=</span><span class="s1">&#39;tf&#39;</span><span class="p">)</span>
<span class="n">idf</span> <span class="o">=</span> <span class="n">IDF</span><span class="p">()</span><span class="o">.</span><span class="n">setInputCol</span><span class="p">(</span><span class="s1">&#39;tf&#39;</span><span class="p">)</span><span class="o">.</span><span class="n">setOutputCol</span><span class="p">(</span><span class="s1">&#39;features&#39;</span><span class="p">)</span>
<span class="n">nb</span>  <span class="o">=</span> <span class="n">NaiveBayes</span><span class="p">()</span>

<span class="n">pipeline</span> <span class="o">=</span> <span class="n">Pipeline</span><span class="p">(</span><span class="n">stages</span><span class="o">=</span><span class="p">[</span><span class="n">cv</span><span class="p">,</span> <span class="n">idf</span><span class="p">,</span> <span class="n">nb</span><span class="p">])</span>

<span class="c1"># Dichiarazione della pipeline</span>
<span class="n">model</span> <span class="o">=</span> <span class="n">pipeline</span><span class="o">.</span><span class="n">fit</span><span class="p">(</span><span class="n">train</span><span class="p">)</span>

<span class="c1"># Valutazione del modello con dati di training</span>
<span class="n">predictions_train</span> <span class="o">=</span> <span class="n">model</span><span class="o">.</span><span class="n">transform</span><span class="p">(</span><span class="n">train</span><span class="p">)</span>

<span class="c1"># Calcolo dell&#39;accuracy</span>
<span class="n">evaluator</span> <span class="o">=</span> <span class="n">MulticlassClassificationEvaluator</span><span class="p">(</span><span class="n">predictionCol</span><span class="o">=</span><span class="s2">&quot;prediction&quot;</span><span class="p">)</span>
<span class="n">eval_train</span> <span class="o">=</span> <span class="n">evaluator</span><span class="o">.</span><span class="n">evaluate</span><span class="p">(</span><span class="n">predictions_train</span><span class="p">)</span>

<span class="c1"># Valutazione del modello con dati di test</span>
<span class="n">predictions_test</span> <span class="o">=</span> <span class="n">model</span><span class="o">.</span><span class="n">transform</span><span class="p">(</span><span class="n">test</span><span class="p">)</span>

<span class="c1"># Calcolo dell&#39;accuracy</span>
<span class="n">evaluator</span> <span class="o">=</span> <span class="n">MulticlassClassificationEvaluator</span><span class="p">(</span><span class="n">predictionCol</span><span class="o">=</span><span class="s2">&quot;prediction&quot;</span><span class="p">)</span>
<span class="n">eval_test</span> <span class="o">=</span> <span class="n">evaluator</span><span class="o">.</span><span class="n">evaluate</span><span class="p">(</span><span class="n">predictions_test</span><span class="p">)</span>
</pre></div>

    </div>
</div>
</div>

</div>
<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<p><a id='4'>
    <h3>SentiWordNet</h3>
</a></p>
<div style="text-align: justify">
<br><b>SENTIWORDNET</b> [4] è una risorsa lessicale estremamente utile in materia di Sentiment Analysis. SENTIWORDNET è il risultato dell'annotazione di ogni synset di WORDNET in accordo alla nozione di "positività", "negatività" e "neutralità". Un <b>synset (synonym set)</b> è un insieme di sinonimi che possono essere descritti da un'unica definizione, perché esprimono uno stesso senso. Una medesima parola, quindi, si può trovare in diversi synset se ha diversi sensi (significati). 
<br>Nell'ambito di SENTIWORDNET, a ogni synset sono associati tre scores, <b>Pos(s)</b>, <b>Neg(s)</b> e <b>Obj(s)</b>, che indicano quanto sono positivi, negativi e oggettivi (cioè neutri) i termini contenuti nel synset. Ciascuno dei tre scores varia nell'intervallo [0, 1] e la loro somma è unitaria per ogni synset.
<br>Ad esempio, il synset <b>[estimable(J,3)]</b> (J nel gergo di WORDNET sta per "aggettivo"), corrispondente al senso "may be computed or estimated" dell'aggettivo "estimable" ha un Obj score pari a 1, mentre Pos e Neg score pari a 0. Al contrario, il synset <b>[estimable(J,1)]</b>, corrispondente al senso “deserving of respect or high regard” ha un Pos score pari a 0.75, un Neg score nullo e un Obj score di 0.25.
</div><p>L'approccio seguito in questo per assegnare un sentimento a ciascun tweet del dataset è composto dai seguenti passi:</p>
<ul>
<li><b>rimozione dei caratteri speciali</b> dalla stringa corrispondente al tweet;</li>
</ul>

</div>
</div>
</div>
<div class="cell border-box-sizing code_cell rendered">
<div class="input">
<div class="prompt input_prompt">In&nbsp;[&nbsp;]:</div>
<div class="inner_cell">
    <div class="input_area">
<div class=" highlight hl-ipython3"><pre><span></span><span class="c1"># remove whitespace</span>
<span class="k">def</span> <span class="nf">remove_all_space</span><span class="p">(</span><span class="n">astring</span><span class="p">):</span>
  <span class="k">return</span> <span class="s2">&quot; &quot;</span><span class="o">.</span><span class="n">join</span><span class="p">(</span><span class="n">astring</span><span class="o">.</span><span class="n">split</span><span class="p">())</span>

<span class="c1"># clean the text </span>
<span class="k">def</span> <span class="nf">remove_features</span><span class="p">(</span><span class="n">data_str</span><span class="p">):</span>
    <span class="c1"># compile regex</span>
    <span class="n">url_re</span> <span class="o">=</span> <span class="n">re</span><span class="o">.</span><span class="n">compile</span><span class="p">(</span><span class="s1">&#39;https?://(www.)?\w+\.\w+(/\w+)*/?&#39;</span><span class="p">)</span>
    <span class="n">punc_re</span> <span class="o">=</span> <span class="n">re</span><span class="o">.</span><span class="n">compile</span><span class="p">(</span><span class="s1">&#39;[</span><span class="si">%s</span><span class="s1">]&#39;</span> <span class="o">%</span> <span class="n">re</span><span class="o">.</span><span class="n">escape</span><span class="p">(</span><span class="n">string</span><span class="o">.</span><span class="n">punctuation</span><span class="p">))</span>
    <span class="n">num_re</span> <span class="o">=</span> <span class="n">re</span><span class="o">.</span><span class="n">compile</span><span class="p">(</span><span class="s1">&#39;(</span><span class="se">\\</span><span class="s1">d+)&#39;</span><span class="p">)</span>
    <span class="n">alpha_num_re</span> <span class="o">=</span> <span class="n">re</span><span class="o">.</span><span class="n">compile</span><span class="p">(</span><span class="s2">&quot;^[a-z0-9_.]+$&quot;</span><span class="p">)</span>
    <span class="c1"># convert to lowercase</span>
    <span class="n">data_str</span> <span class="o">=</span> <span class="n">data_str</span><span class="o">.</span><span class="n">lower</span><span class="p">()</span>
    <span class="c1"># remove hyperlinks</span>
    <span class="n">data_str</span> <span class="o">=</span> <span class="n">url_re</span><span class="o">.</span><span class="n">sub</span><span class="p">(</span><span class="s1">&#39; &#39;</span><span class="p">,</span> <span class="n">data_str</span><span class="p">)</span>
    <span class="c1"># remove puncuation</span>
    <span class="n">data_str</span> <span class="o">=</span> <span class="n">punc_re</span><span class="o">.</span><span class="n">sub</span><span class="p">(</span><span class="s1">&#39; &#39;</span><span class="p">,</span> <span class="n">data_str</span><span class="p">)</span>
    <span class="c1"># remove numeric &#39;words&#39;</span>
    <span class="n">data_str</span> <span class="o">=</span> <span class="n">num_re</span><span class="o">.</span><span class="n">sub</span><span class="p">(</span><span class="s1">&#39; &#39;</span><span class="p">,</span> <span class="n">data_str</span><span class="p">)</span>
    <span class="c1"># remove non a-z 0-9 characters and words shorter than 3 characters</span>
    <span class="n">list_pos</span> <span class="o">=</span> <span class="mi">0</span>
    <span class="n">cleaned_str</span> <span class="o">=</span> <span class="s1">&#39;&#39;</span>
    <span class="k">for</span> <span class="n">word</span> <span class="ow">in</span> <span class="n">data_str</span><span class="o">.</span><span class="n">split</span><span class="p">():</span>
        <span class="k">if</span> <span class="n">list_pos</span> <span class="o">==</span> <span class="mi">0</span><span class="p">:</span>
            <span class="k">if</span> <span class="n">alpha_num_re</span><span class="o">.</span><span class="n">match</span><span class="p">(</span><span class="n">word</span><span class="p">)</span> <span class="ow">and</span> <span class="nb">len</span><span class="p">(</span><span class="n">word</span><span class="p">)</span> <span class="o">&gt;</span> <span class="mi">2</span><span class="p">:</span>
                <span class="n">cleaned_str</span> <span class="o">=</span> <span class="n">word</span>
            <span class="k">else</span><span class="p">:</span>
                <span class="n">cleaned_str</span> <span class="o">=</span> <span class="s1">&#39; &#39;</span>
        <span class="k">else</span><span class="p">:</span>
            <span class="k">if</span> <span class="n">alpha_num_re</span><span class="o">.</span><span class="n">match</span><span class="p">(</span><span class="n">word</span><span class="p">)</span> <span class="ow">and</span> <span class="nb">len</span><span class="p">(</span><span class="n">word</span><span class="p">)</span> <span class="o">&gt;</span> <span class="mi">2</span><span class="p">:</span>
                <span class="n">cleaned_str</span> <span class="o">=</span> <span class="n">cleaned_str</span> <span class="o">+</span> <span class="s1">&#39; &#39;</span> <span class="o">+</span> <span class="n">word</span>
            <span class="k">else</span><span class="p">:</span>
                <span class="n">cleaned_str</span> <span class="o">+=</span> <span class="s1">&#39; &#39;</span>
        <span class="n">list_pos</span> <span class="o">+=</span> <span class="mi">1</span>
    <span class="n">cleaned_str2</span> <span class="o">=</span> <span class="n">remove_all_space</span><span class="p">(</span><span class="n">cleaned_str</span><span class="p">)</span>
    <span class="k">return</span> <span class="n">cleaned_str2</span>
</pre></div>

    </div>
</div>
</div>

</div>
<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<ul>
<li><b>tokenizzazione</b> e <b>rimozione delle stopwords</b>;</li>
</ul>

</div>
</div>
</div>
<div class="cell border-box-sizing code_cell rendered">
<div class="input">
<div class="prompt input_prompt">In&nbsp;[&nbsp;]:</div>
<div class="inner_cell">
    <div class="input_area">
<div class=" highlight hl-ipython3"><pre><span></span><span class="c1"># Tokenizzazione</span>
<span class="n">tkn</span> <span class="o">=</span> <span class="n">Tokenizer</span><span class="p">()</span>\
      <span class="o">.</span><span class="n">setInputCol</span><span class="p">(</span><span class="s2">&quot;cleaned_text&quot;</span><span class="p">)</span>\
      <span class="o">.</span><span class="n">setOutputCol</span><span class="p">(</span><span class="s2">&quot;words&quot;</span><span class="p">)</span>

<span class="c1"># Eliminazione Stopwords</span>
<span class="n">englishStopWords</span> <span class="o">=</span> <span class="n">StopWordsRemover</span><span class="o">.</span><span class="n">loadDefaultStopWords</span><span class="p">(</span><span class="s2">&quot;english&quot;</span><span class="p">)</span>
<span class="n">stops</span> <span class="o">=</span> <span class="n">StopWordsRemover</span><span class="p">()</span>\
        <span class="o">.</span><span class="n">setStopWords</span><span class="p">(</span><span class="n">englishStopWords</span><span class="p">)</span>\
        <span class="o">.</span><span class="n">setInputCol</span><span class="p">(</span><span class="s2">&quot;words&quot;</span><span class="p">)</span>\
        <span class="o">.</span><span class="n">setOutputCol</span><span class="p">(</span><span class="s2">&quot;words_nsw&quot;</span><span class="p">)</span>

<span class="n">pipeline</span> <span class="o">=</span> <span class="n">Pipeline</span><span class="p">(</span><span class="n">stages</span> <span class="o">=</span> <span class="p">[</span><span class="n">tkn</span><span class="p">,</span> <span class="n">stops</span><span class="p">])</span>

<span class="n">df_TweetsCleaned</span> <span class="o">=</span> <span class="n">pipeline</span>\
    <span class="o">.</span><span class="n">fit</span><span class="p">(</span><span class="n">df_TweetsCleaned</span><span class="o">.</span><span class="n">select</span><span class="p">(</span><span class="s2">&quot;full_text&quot;</span><span class="p">,</span> <span class="s2">&quot;cleaned_text&quot;</span><span class="p">))</span>\
    <span class="o">.</span><span class="n">transform</span><span class="p">(</span><span class="n">df_TweetsCleaned</span><span class="o">.</span><span class="n">select</span><span class="p">(</span><span class="s2">&quot;full_text&quot;</span><span class="p">,</span> <span class="s2">&quot;cleaned_text&quot;</span><span class="p">))</span>

<span class="n">df_TweetsCleaned</span><span class="o">.</span><span class="n">select</span><span class="p">(</span><span class="s2">&quot;full_text&quot;</span><span class="p">,</span> <span class="s2">&quot;cleaned_text&quot;</span><span class="p">)</span><span class="o">.</span><span class="n">show</span><span class="p">()</span>
</pre></div>

    </div>
</div>
</div>

</div>
<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<ul>
<li><b>estrazione del tag dai token</b>. Con questo si intende effettuare una sorta di analisi grammaticale del testo per capire se un termine è un nome, un aggettivo, un verbo o un avverbio. Questa informazione è utile sia per la corretta lemmatizzazione (vedi punto successivo), sia per estrarre il corretto synset da SENTIWORDNET. Il metodo pos_tag del pacchetto nltk fa proprio al caso nostro. Ad esempio, se abbiamo una lista ('play','cards'), il risultato sarà (('play','v'), ('cards','n'));</li>
</ul>

</div>
</div>
</div>
<div class="cell border-box-sizing code_cell rendered">
<div class="input">
<div class="prompt input_prompt">In&nbsp;[&nbsp;]:</div>
<div class="inner_cell">
    <div class="input_area">
<div class=" highlight hl-ipython3"><pre><span></span><span class="c1"># extract part of speech</span>
<span class="k">def</span> <span class="nf">pos</span><span class="p">(</span><span class="n">tokenized_text</span><span class="p">):</span>
    <span class="n">sent_tag_list</span> <span class="o">=</span> <span class="n">pos_tag</span><span class="p">(</span><span class="n">tokenized_text</span><span class="p">)</span> 
    <span class="n">aList</span> <span class="o">=</span> <span class="p">[]</span>
    <span class="k">for</span> <span class="n">word</span><span class="p">,</span> <span class="n">tag</span> <span class="ow">in</span> <span class="n">sent_tag_list</span><span class="p">:</span>
        <span class="n">tagToUse</span> <span class="o">=</span> <span class="s1">&#39;&#39;</span>
        <span class="k">if</span> <span class="n">tag</span><span class="o">.</span><span class="n">startswith</span><span class="p">(</span><span class="s1">&#39;J&#39;</span><span class="p">):</span>
            <span class="n">tagToUse</span><span class="o">=</span> <span class="s1">&#39;a&#39;</span> <span class="c1"># aggettivi</span>
        <span class="k">elif</span> <span class="n">tag</span><span class="o">.</span><span class="n">startswith</span><span class="p">(</span><span class="s1">&#39;N&#39;</span><span class="p">):</span>
            <span class="n">tagToUse</span><span class="o">=</span> <span class="s1">&#39;n&#39;</span> <span class="c1"># sostantivi</span>
        <span class="k">elif</span> <span class="n">tag</span><span class="o">.</span><span class="n">startswith</span><span class="p">(</span><span class="s1">&#39;R&#39;</span><span class="p">):</span>
            <span class="n">tagToUse</span><span class="o">=</span> <span class="s1">&#39;r&#39;</span> <span class="c1"># avverbi</span>
        <span class="k">elif</span> <span class="n">tag</span><span class="o">.</span><span class="n">startswith</span><span class="p">(</span><span class="s1">&#39;V&#39;</span><span class="p">):</span>
            <span class="n">tagToUse</span><span class="o">=</span> <span class="s1">&#39;v&#39;</span> <span class="c1"># verbi</span>
        <span class="k">else</span><span class="p">:</span>
            <span class="k">continue</span>
        <span class="n">aList</span><span class="o">.</span><span class="n">append</span><span class="p">((</span><span class="n">word</span><span class="p">,</span> <span class="n">tagToUse</span><span class="p">))</span>
    <span class="k">return</span> <span class="n">aList</span>
</pre></div>

    </div>
</div>
</div>

</div>
<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<ul>
<li><b>lemmatizzazione</b>, ovvero il processo di conversione di una parola nella sua forma base. La differenza con lo stemming è che questo rimuove solo gli ultimi caratteri di una parola, portando spesso a una forma sbagliata, mentre la lemmatizzazione considera il contesto della parola, estraendo quindi il corretto significato;</li>
</ul>

</div>
</div>
</div>
<div class="cell border-box-sizing code_cell rendered">
<div class="input">
<div class="prompt input_prompt">In&nbsp;[&nbsp;]:</div>
<div class="inner_cell">
    <div class="input_area">
<div class=" highlight hl-ipython3"><pre><span></span><span class="c1"># lemmatize the tokens </span>
<span class="n">lemmatizer</span> <span class="o">=</span> <span class="n">WordNetLemmatizer</span><span class="p">()</span>
<span class="k">def</span> <span class="nf">lemmatize</span><span class="p">(</span><span class="n">array_of_word_for_a_comment</span><span class="p">):</span>
    <span class="n">all_words_in_comment</span> <span class="o">=</span> <span class="p">[]</span>
    <span class="k">for</span> <span class="n">word</span> <span class="ow">in</span> <span class="n">array_of_word_for_a_comment</span><span class="p">:</span>
        <span class="n">lemma</span> <span class="o">=</span> <span class="n">lemmatizer</span><span class="o">.</span><span class="n">lemmatize</span><span class="p">(</span><span class="n">word</span><span class="p">[</span><span class="mi">0</span><span class="p">],</span> <span class="n">pos</span><span class="o">=</span><span class="n">word</span><span class="p">[</span><span class="mi">1</span><span class="p">])</span>
        <span class="k">if</span> <span class="ow">not</span> <span class="n">lemma</span><span class="p">:</span>
            <span class="k">continue</span>
        <span class="n">all_words_in_comment</span><span class="o">.</span><span class="n">append</span><span class="p">([</span><span class="n">lemma</span><span class="p">,</span><span class="n">word</span><span class="p">[</span><span class="mi">1</span><span class="p">]])</span>  
    <span class="k">return</span> <span class="n">all_words_in_comment</span>
</pre></div>

    </div>
</div>
</div>

</div>
<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<ul>
<li><b>calcolo dello score</b> di una frase. Lo score di un tweet è dato dalla somma dei contributi (in termini di positive, negative e neutral score) di tutti i token contenuti al suo interno e in SENTIWORDNET;</li>
</ul>

</div>
</div>
</div>
<div class="cell border-box-sizing code_cell rendered">
<div class="input">
<div class="prompt input_prompt">In&nbsp;[&nbsp;]:</div>
<div class="inner_cell">
    <div class="input_area">
<div class=" highlight hl-ipython3"><pre><span></span><span class="c1">#calculate the sentiment </span>
<span class="k">def</span> <span class="nf">cal_score</span><span class="p">(</span><span class="n">array_of_lemma_tag_for_a_comment</span><span class="p">):</span>
    <span class="n">alist</span> <span class="o">=</span> <span class="p">[</span><span class="n">array_of_lemma_tag_for_a_comment</span><span class="p">]</span>
    <span class="n">totalScore</span> <span class="o">=</span> <span class="mi">0</span>
    <span class="n">count_words_included</span> <span class="o">=</span> <span class="mi">0</span>
    <span class="k">for</span> <span class="n">word</span> <span class="ow">in</span> <span class="n">array_of_lemma_tag_for_a_comment</span><span class="p">:</span>
        <span class="n">synset_forms</span> <span class="o">=</span> <span class="nb">list</span><span class="p">(</span><span class="n">swn</span><span class="o">.</span><span class="n">senti_synsets</span><span class="p">(</span><span class="n">word</span><span class="p">[</span><span class="mi">0</span><span class="p">],</span> <span class="n">word</span><span class="p">[</span><span class="mi">1</span><span class="p">]))</span>
        <span class="k">if</span> <span class="ow">not</span> <span class="n">synset_forms</span><span class="p">:</span>
            <span class="k">continue</span>
        <span class="n">synset</span> <span class="o">=</span> <span class="n">synset_forms</span><span class="p">[</span><span class="mi">0</span><span class="p">]</span> 
        <span class="n">totalScore</span> <span class="o">=</span> <span class="n">totalScore</span> <span class="o">+</span> <span class="n">synset</span><span class="o">.</span><span class="n">pos_score</span><span class="p">()</span> <span class="o">-</span> <span class="n">synset</span><span class="o">.</span><span class="n">neg_score</span><span class="p">()</span>
        <span class="n">count_words_included</span> <span class="o">=</span> <span class="n">count_words_included</span> <span class="o">+</span><span class="mi">1</span>
    <span class="n">final_dec</span> <span class="o">=</span> <span class="s1">&#39;&#39;</span>
    <span class="k">if</span> <span class="n">count_words_included</span> <span class="o">==</span> <span class="mi">0</span><span class="p">:</span>
        <span class="n">final_dec</span> <span class="o">=</span> <span class="s1">&#39;N/A&#39;</span>
    <span class="k">elif</span> <span class="n">totalScore</span> <span class="o">==</span> <span class="mi">0</span><span class="p">:</span>
        <span class="n">final_dec</span> <span class="o">=</span> <span class="s1">&#39;Neu&#39;</span>        
    <span class="k">elif</span> <span class="n">totalScore</span><span class="o">/</span><span class="n">count_words_included</span> <span class="o">&lt;</span> <span class="mi">0</span><span class="p">:</span>
        <span class="n">final_dec</span> <span class="o">=</span> <span class="s1">&#39;Neg&#39;</span>
    <span class="k">elif</span> <span class="n">totalScore</span><span class="o">/</span><span class="n">count_words_included</span> <span class="o">&gt;</span> <span class="mi">0</span><span class="p">:</span>
        <span class="n">final_dec</span> <span class="o">=</span> <span class="s1">&#39;Pos&#39;</span>
    <span class="k">return</span> <span class="n">final_dec</span>
</pre></div>

    </div>
</div>
</div>

</div>
<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<p><br>Tutte le funzioni definite sono poi state registrate come udf (User Defined Function) in Spark.</p>

</div>
</div>
</div>
<div class="cell border-box-sizing code_cell rendered">
<div class="input">
<div class="prompt input_prompt">In&nbsp;[&nbsp;]:</div>
<div class="inner_cell">
    <div class="input_area">
<div class=" highlight hl-ipython3"><pre><span></span><span class="n">remove_features_udf</span> <span class="o">=</span> <span class="n">udf</span><span class="p">(</span><span class="n">remove_features</span><span class="p">,</span> <span class="n">StringType</span><span class="p">())</span>
<span class="n">pos_udf</span> <span class="o">=</span> <span class="n">udf</span><span class="p">(</span><span class="n">pos</span><span class="p">,</span><span class="n">ArrayType</span><span class="p">(</span><span class="n">StructType</span><span class="p">([</span> <span class="n">StructField</span><span class="p">(</span><span class="s2">&quot;word&quot;</span><span class="p">,</span> <span class="n">StringType</span><span class="p">(),</span> <span class="kc">False</span><span class="p">),</span> <span class="n">StructField</span><span class="p">(</span><span class="s2">&quot;tag&quot;</span><span class="p">,</span> <span class="n">StringType</span><span class="p">(),</span> <span class="kc">False</span><span class="p">)])))</span>
<span class="n">lemmatize_udf</span> <span class="o">=</span> <span class="n">udf</span><span class="p">(</span><span class="n">lemmatize</span><span class="p">,</span><span class="n">ArrayType</span><span class="p">(</span><span class="n">StructType</span><span class="p">([</span> <span class="n">StructField</span><span class="p">(</span><span class="s2">&quot;lemma&quot;</span><span class="p">,</span> <span class="n">StringType</span><span class="p">(),</span> <span class="kc">False</span><span class="p">),</span> <span class="n">StructField</span><span class="p">(</span><span class="s2">&quot;tag&quot;</span><span class="p">,</span> <span class="n">StringType</span><span class="p">(),</span> <span class="kc">False</span><span class="p">)])))</span>
<span class="n">cal_score_udf</span> <span class="o">=</span> <span class="n">udf</span><span class="p">(</span><span class="n">cal_score</span><span class="p">,</span><span class="n">StringType</span><span class="p">())</span>
</pre></div>

    </div>
</div>
</div>

</div>
<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<p><a id='5'>
    <h3>Analisi delle performance di Spark</h3>
</a></p>
<div style="text-align: justify">
<br> In questa sezione riportiamo alcune considerazioni riguardanti le performance di Spark. Sebbene sia stata utilizzata una macchina virtuale e i tempi siano stati misurati sfruttando la direttiva time fornita da Jupyter (che, per la precisione, misura il cosiddetto wall time, ovvero non solo il tempo di utilizzo della CPU del singolo processo, ma anche quello dovuto all'interferenza di altri processi concorrenti), è comunque evidente il vantaggio nell'utilizzo di questo framework. Per valutare le performance, sono state confrontate due implementazioni dell'etichettatura tramite il modello VADER: una in pyspark (riportata sopra) e una in Python senza Spark. In particolare, per quanto riguarda la seconda, ne sono state preparate tre versioni: una prima che non sfrutta meccanismi di parallelizzazione del codice (usando libreria pandas); una seconda analoga alla prima ma che non usa pandas; una terza basata sulla libreria Multiprocessing per trarre beneficio dei più core sulla macchina per l'esecuzione delle funzioni sul DataFrame Pandas.
</div>
</div>
</div>
</div>
<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<p><h5>Esecuzioni</h5></p>
<div style="text-align: justify">

<br>Abbiamo eseguito l'algoritmo in ambiente Spark, usando il contesto di default creato all'avvio del notebook Jupyter. In tale situazione, Spark crea tanti worker (threads) quanti sono i core logici sulla macchina (local[*]). In questo caso, il tempo di esecuzione su 43710 tweet è pari circa a 18s.
Il passo successivo è stato quello di eseguire le due versioni senza Spark. I risultati sono riportati di seguito:
</div>
</div>
</div>
</div>
<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<ul>
<li>Python Pandas = 772s</li>
<li>Python puro = 719s</li>
</ul>
<div style="text-align: justify">
All'atto della creazione del DataFrame, Spark suddivide i dati su più partizioni (di default 200) in modo tale che tutti i worker lavorino sulla propria partizione in parallelo. Il programmatore ha due possibilità: la prima consiste nell'ignorare aspetti relativi alla concorrenza e alla configurazione della macchina (o del cluster) su cui esegue il codice; la seconda consiste nella scelta del numero di worker e del numero di partizioni in cui suddividere il dataframe. Nel secondo caso, è possibile lanciare pyspark con l'opzione --master local[K], dove K è proprio il numero di worker thread desiderato (idealmente pari al numero di core della macchina) e, nel codice, tramite oppurtune funzioni (parallelize(), repartition(), ...) controllare il numero di partizioni.
Poiché la macchina virtuale gira su due core fisici, abbiamo lanciato pyspark specificando 2 worker e suddiviso il dataframe in 2 partizioni. Il tempo d'esecuzione è stato di 345s.
Allo stesso modo, abbiamo lanciato lo script python con la libreria Multiprocessing indicando 2 come grado parallelizzazione. Il risultato ottenuto è stato di 424s.
<br>Infine, i tempi relativi all'esecuzione su Spark potrebbero anche coinvolgere operazioni di lettura da MongoDB per la sua caratteristica di lazy evaluation. Invece, i tempi relativi alle esecuzioni "no Spark" si riferiscono esclusivamente all'etichettatura.

Chiaramente, questa analisi non è stata svolta in modo accurato, ma nonostante ciò è evidente che l'astrazione dei DataFrame Spark nasconda meccanismi per parallelizzare l'esecuzione delle istruzioni in modo trasparente al programmatore, rendendolo più efficiente rispetto alle altre soluzioni provate.
</div>
</div>
</div>
</div>
<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<p><a id='7'>
    <h3>Installazione Spark e MongoDB</h3>
</a></p>
<p><h5>Installazione Apache Spark</h5>
<br>Per quanto riguarda l’installazione di Apache Spark in locale, una possibilità è quella di utilizzare una virtual machine, in modo da isolare completamente Spark dal sistema operativo host, mantenendo comunque la possibilità di eseguire script PySpark su dei notebook Jupyter. 
Installata una macchina virtuale Linux (ad esempio XUbuntu), è possibile installare correttamente Spark seguendo le indicazioni qui riportate:<br></p>
<ul>
<li>Scaricare l’ultima versione di Spark dal sito ufficiale.</li>
<li>Creare la cartella spark  in <span style="background: rgb(255, 0, 87); border-radius: 5px !important; color: white; padding: 3px">/usr/lib/</span> contenente tutti i file di Spark;</li>
<li>Installare SBT;</li>
<li>Installare Java 8;</li>
<li>Configurare Spark attraverso il file <span style="background: rgb(255, 0, 87); border-radius: 5px !important; color: white; padding: 3px">/usr/lib/spark/conf/spark-env.sh</span> (eventualmente generarlo med iante template presente nella stessa cartella), aggiungendo le righe:<ul>
<li><span style="font-family: 'Andale Mono'">JAVA_HOME=/usr/lib/jvm/java-8-oracle</span></li>
<li><span style="font-family: 'Andale Mono'">SPARK_WORKER_MEMORY=4g</span></li>
</ul>
</li>
<li>Installare Anaconda 3;&lt;/span&gt;</li>
<li>Modificare le variabili d’ambiente nel file ~/.bashrc :<ul>
<li><span style="font-family: 'Andale Mono'">export JAVA_HOME=/usr/lib/jvm/java-8-oracle </span> </li>
<li><span style="font-family: 'Andale Mono'">export SBT_HOME=/usr/share/sbt-launcher-packaging/bin/sbt-launch.jar  </span></li>
<li><span style="font-family: 'Andale Mono'">export SPARK_HOME=/usr/lib/spark</span></li>
<li><span style="font-family: 'Andale Mono'">export PATH=<span>&#36;</span>PATH:<span>&#36;</span>JAVA_HOME/bin&lt;/span&gt;</li>
<li><span style="font-family: 'Andale Mono'">export PATH=<span>&#36;</span>PATH:<span>&#36;</span>SBT_HOME/bin:<span>&#36;</span>SPARK_HOME/bin:<span>&#36;</span>SPARK_HOME/sbin&lt;/span&gt;</li>
<li><span style="font-family: 'Andale Mono'">export PYSPARK_DRIVER_PYTHON=jupyter</span></li>
<li><span style="font-family: 'Andale Mono'">export PYSPARK_DRIVER_PYTHON_OPTS='notebook'</span></li>
<li><span style="font-family: 'Andale Mono'">export PYSPARK_PYTHON=python2.7</span></li>
<li><span style="font-family: 'Andale Mono'">export PYTHONPATH=<span>&#36;</span>SPARK_HOME/python:<span>&#36;</span>PYTHONPATH&lt;/span&gt;</li>
</ul>
</li>
<li>Verificare la corretta installazione lanciando il commando pyspark sul terminale e visualizzato il notebook Jupyter a localhost:8888 (aperto automaticamente).</li>
</ul>
<p>La lista dettagliata dei comandi da utilizzare è presente nella guida [1]. 
In definitiva, la configurazione utilizzata per l’elaborato è dunque la seguente:
<br></p>
<ul>
<li>Spark 2.4.5 - Hadoop2.7</li>
<li>XUbuntu 20.04 - 4 GB di RAM, 2 CPU</li>
<li>Anaconda 3 per i packages necessari</li>
</ul>
<p><h5>Installazione MongoDB</h5>
<br>Il database NoSQL MongoDB è stato installato sulla macchina host. Per l'installazione è stato necessario scaricare i file sorgenti dal sito ufficiale e seguire documentazione [2]. 
In particolare è stata utilizzata la versione 2.4.2 Community Server scaricabile a [3]. Scaricato il necessario, per lanciare il server è sufficiente spostarsi nella cartella &lt;span <span style="background: rgb(255, 0, 87); border-radius: 5px !important; color: white; padding: 3px">/mongodb/bin/</span> e lanciare il comando</p>
<ul>
<li><span style="font-family: 'Andale Mono'">./mongod --bind_ip localhost,[IP ADDRESS] --dbpath [Path-to-dbFolder]</span></li>
</ul>
<p><h5>Connessione Spark-MongoDB</h5>
<br>Avendo installato Spark in locale, è possibile dunque caricare direttamente dal database i file necessari per le elaborazioni in PySpark su notebook Jupyter, utilizzando il MongoDB Spark Connector [4]. Per installare il connettore è sufficiente copiare nella cartella <span style="background: rgb(255, 0, 87); border-radius: 5px !important; color: white; padding: 3px">/usr/lib/spark/jars/</span> i seguenti file (disponibili nel repository GitHub):<br></p>
<ul>
<li>bson-3.8.1.jar</li>
<li>mongodb-driver-core-3.8.1.jar</li>
<li>mongodb-driver-3.8.1.jar</li>
<li>mongo-spark-connector_2.11-2.4.2.jar</li>
</ul>
<p>Una volta caricati i jar necessari, per poter caricare un DataFrame con il contenuto di una collection presente su MongoDB è necessario utilizzare una SparkSession:<br></p>

</div>
</div>
</div>
<div class="cell border-box-sizing code_cell rendered">
<div class="input">
<div class="prompt input_prompt">In&nbsp;[&nbsp;]:</div>
<div class="inner_cell">
    <div class="input_area">
<div class=" highlight hl-ipython3"><pre><span></span><span class="n">spark</span> <span class="o">=</span> <span class="n">SparkSession</span>\
    <span class="o">.</span><span class="n">builder</span>\
    <span class="o">.</span><span class="n">config</span><span class="p">(</span><span class="s2">&quot;spark.mongodb.input.uri&quot;</span><span class="p">,</span> <span class="s2">&quot;mongodb://[MongoDB IP ADDRESS]/[DatabaseName].[CollectionName]?retryWrites=true&quot;</span><span class="p">)</span>\
    <span class="o">.</span><span class="n">config</span><span class="p">(</span><span class="s2">&quot;spark.mongodb.output.uri&quot;</span><span class="p">,</span> <span class="s2">&quot;mongodb://[MongoDB IP ADDRESS]/[DatabaseName].[CollectionName]?retryWrites=true&quot;</span><span class="p">)</span>\
    <span class="o">.</span><span class="n">getOrCreate</span><span class="p">()</span>
<span class="n">df</span> <span class="o">=</span> <span class="n">spark</span><span class="o">.</span><span class="n">read</span><span class="o">.</span><span class="n">format</span><span class="p">(</span><span class="s2">&quot;com.mongodb.spark.sql.DefaultSource&quot;</span><span class="p">)</span><span class="o">.</span><span class="n">load</span><span class="p">()</span>
</pre></div>

    </div>
</div>
</div>

</div>
<div class="cell border-box-sizing text_cell rendered"><div class="prompt input_prompt">
</div><div class="inner_cell">
<div class="text_cell_render border-box-sizing rendered_html">
<p><a id='8'>
    <h3>Riferimenti</h3>
</a></p>
<p>[1] Probabilistic Approaches for Sentiment Analysis: Latent Dirichlet Allocation for Ontology Building and Sentiment Extraction, Colace F., De Santo M.</p>
<p>[2] GeoCoV19: A Dataset of Hundreds of Millions of Multilingual COVID-19 Tweets with Location Information</p>
<p>[3] VADER: A Parsimonious Rule-based Model for Sentiment Analysis of Social Media Text</p>
<p>[4] SENTIWORDNET 3.0: An Enhanced Lexical Resource for Sentiment Analysis and Opinion Mining</p>
<p>[5] What is TF-IDF: <a href="https://www.onely.com/blog/what-is-tf-idf/">https://www.onely.com/blog/what-is-tf-idf/</a></p>
<h5>Riferimenti per l'installazione</h5><p>[6]<a href="https://medium.com/@brajendragouda/installing-apache-spark-on-ubuntu-pyspark-on-juputer-ca8e40e8e655">https://medium.com/@brajendragouda/installing-apache-spark-on-ubuntu-pyspark-on-juputer-ca8e40e8e655</a></p>
<p>[7]<a href="https://docs.mongodb.com/manual/tutorial/install-mongodb-on-windows/">https://docs.mongodb.com/manual/tutorial/install-mongodb-on-windows/</a></p>
<p>[8]<a href="https://www.mongodb.com/try/download/community">https://www.mongodb.com/try/download/community</a></p>
<p>[9]<a href="https://docs.mongodb.com/spark-connector/master/">https://docs.mongodb.com/spark-connector/master/</a></p>

</div>
</div>
</div>
<div class="cell border-box-sizing code_cell rendered">
<div class="input">
<div class="prompt input_prompt">In&nbsp;[&nbsp;]:</div>
<div class="inner_cell">
    <div class="input_area">
<div class=" highlight hl-ipython3"><pre><span></span> 
</pre></div>

    </div>
</div>
</div>

</div>
    </div>
  </div>