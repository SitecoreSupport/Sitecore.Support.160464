namespace Sitecore.Support.ContentSearch.LuceneProvider
{
    using Lucene.Net.Index;
    using Lucene.Net.Search;
    using Lucene.Net.Store;
    using Sitecore.ContentSearch;
    using Sitecore.ContentSearch.Abstractions;
    using Sitecore.ContentSearch.Diagnostics;
    using Sitecore.ContentSearch.Events;
    using Sitecore.ContentSearch.LuceneProvider;
    using Sitecore.ContentSearch.LuceneProvider.Sharding;
    using Sitecore.ContentSearch.Maintenance;
    using Sitecore.ContentSearch.Maintenance.Strategies;
    using Sitecore.ContentSearch.Security;
    using Sitecore.ContentSearch.Sharding;
    using Sitecore.ContentSearch.Utilities;
    using Sitecore.Diagnostics;
    using Sitecore.IO;
    using Sitecore.Xml;
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Runtime.CompilerServices;
    using System.Runtime.InteropServices;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Xml;

    public class LuceneIndex : AbstractSearchIndex, ILuceneProviderIndex, ISearchIndex, IDisposable, ILuceneProviderSearchable
    {
        private AbstractFieldNameTranslator fieldNameTranslator;
        protected object indexUpdateLock;
        private bool isSharded;
        private LuceneUpdateContext lastUpdateContextCreated;
        private readonly string name;
        private readonly LuceneIndexSchema schema;
        private readonly IContentSearchConfigurationSettings settings;
        private readonly IShardFactory shardFactory;
        private readonly Dictionary<string, string> shardRootFolderPaths;
        internal Dictionary<int, LuceneShard> shards;
        private readonly List<IIndexUpdateStrategy> strategies;
        private readonly LuceneIndexSummary summary;

        protected LuceneIndex(string name) : base(null)
        {
            this.shards = new Dictionary<int, LuceneShard>();
            this.shardRootFolderPaths = new Dictionary<string, string>();
            this.shardFactory = new LuceneShardFactory();
            this.strategies = new List<IIndexUpdateStrategy>();
            this.indexUpdateLock = new object();
            Sitecore.Diagnostics.Assert.ArgumentNotNullOrEmpty(name, "name");
            this.name = name;
            this.summary = new LuceneIndexSummary(this);
            this.schema = new LuceneIndexSchema(this);
            this.settings = base.Locator.GetInstance<IContentSearchConfigurationSettings>();
        }

        public LuceneIndex(string name, string folder, IIndexPropertyStore propertyStore) : this(name, folder, propertyStore, null)
        {
        }

        public LuceneIndex(string name, string folder, IIndexPropertyStore propertyStore, string group) : base(group)
        {
            this.shards = new Dictionary<int, LuceneShard>();
            this.shardRootFolderPaths = new Dictionary<string, string>();
            this.shardFactory = new LuceneShardFactory();
            this.strategies = new List<IIndexUpdateStrategy>();
            this.indexUpdateLock = new object();
            Sitecore.Diagnostics.Assert.ArgumentNotNullOrEmpty(name, "name");
            Sitecore.Diagnostics.Assert.ArgumentNotNullOrEmpty(folder, "folder");
            Sitecore.Diagnostics.Assert.ArgumentNotNull(propertyStore, "propertyStore");
            this.name = name;
            this.FolderName = folder;
            this.PropertyStore = propertyStore;
            this.summary = new LuceneIndexSummary(this);
            this.schema = new LuceneIndexSchema(this);
            this.settings = base.Locator.GetInstance<IContentSearchConfigurationSettings>();
        }

        public void AddShardFolderPath(XmlNode configNode)
        {
            base.VerifyNotDisposed();
            Sitecore.Diagnostics.Assert.ArgumentNotNull(configNode, "configNode");
            string attribute = Sitecore.Xml.XmlUtil.GetAttribute("shardName", configNode);
            string shardRootFolderPath = Sitecore.Xml.XmlUtil.GetAttribute("shardRootFolderPath", configNode);
            this.AddShardFolderPath(attribute, shardRootFolderPath);
        }

        public void AddShardFolderPath(string shardName, string shardRootFolderPath)
        {
            base.VerifyNotDisposed();
            Sitecore.Diagnostics.Assert.IsNotNullOrEmpty(shardName, "shardName is null or empty.");
            Sitecore.Diagnostics.Assert.IsNotNullOrEmpty(shardRootFolderPath, "shardFolderPath is null or empty.");
            this.shardRootFolderPaths[shardName] = shardRootFolderPath;
        }

        public override void AddStrategy(IIndexUpdateStrategy strategy)
        {
            base.VerifyNotDisposed();
            Sitecore.Diagnostics.Assert.ArgumentNotNull(strategy, "The strategy cannot be null");
            this.strategies.Add(strategy);
        }

        public override IProviderDeleteContext CreateDeleteContext()
        {
            this.EnsureInitialized();
            LuceneDeleteContext context = new LuceneDeleteContext(this);
            this.lastUpdateContextCreated = context;
            return context;
        }

        public virtual Lucene.Net.Store.Directory CreateDirectory(string folder)
        {
            Sitecore.Diagnostics.Assert.ArgumentNotNullOrEmpty(folder, "folder");
            this.EnsureInitialized();
            DirectoryInfo path = new DirectoryInfo(folder);
            Sitecore.IO.FileUtil.EnsureFolder(folder);
            FSDirectory directory = FSDirectory.Open(path, this.CreateLockFactory(path.FullName));
            lock (this)
            {
                if (!IndexReader.IndexExists(directory))
                {
                    new IndexWriter(directory, ((LuceneIndexConfiguration)this.Configuration).Analyzer, true, IndexWriter.MaxFieldLength.UNLIMITED).Dispose();
                }
            }
            return directory;
        }

        protected virtual IProviderUpdateContext CreateFullRebuildContext() =>
            this.CreateUpdateContext();

        protected internal virtual LockFactory CreateLockFactory(string path)
        {
            base.VerifyNotDisposed();
            return new DiagnosticLockFactory(new NativeFSLockFactory(path));
        }

        protected virtual IndexReader CreateMultiReader(IEnumerable<IndexReader> readers)
        {
            Sitecore.Diagnostics.Assert.ArgumentNotNull(readers, "readers");
            this.EnsureInitialized();
            IndexReader[] subReaders = readers.ToArray<IndexReader>();
            Sitecore.Diagnostics.Assert.ArgumentCondition(subReaders.Length != 0, "readers", "readers is empty.");
            if (subReaders.Length == 1)
            {
                return subReaders[0];
            }
            return new MultiReader(subReaders, true);
        }

        public virtual IndexReader CreateReader(LuceneIndexAccess indexAccess)
        {
            List<IndexReader> readers = new List<IndexReader>(this.shards.Count);
            readers.AddRange(from shard in this.shards.Values.ToList<LuceneShard>() select this.CreateReader(shard, indexAccess));
            return this.CreateMultiReader(readers);
        }

        public IndexReader CreateReader(Shard shard, LuceneIndexAccess indexAccess)
        {
            LuceneShard shard2;
            this.EnsureInitialized();
            if (!this.shards.TryGetValue(shard.Id, out shard2))
            {
                throw new ArgumentException("Invalid shard");
            }
            return shard2.CreateReader(indexAccess);
        }

        public override IProviderSearchContext CreateSearchContext(SearchSecurityOptions securityOptions = 0)
        {
            this.EnsureInitialized();
            return new LuceneSearchContext(this, (IEnumerable<ILuceneProviderSearchable>)this.shards.Values, securityOptions);
        }

        public virtual Searcher CreateSearcher(LuceneIndexAccess indexAccess)
        {
            this.EnsureInitialized();
            if (!this.IsSharded)
            {
                return this.CreateSearcher(this.shards.Values.First<LuceneShard>(), indexAccess);
            }
            IndexReader[] readers = (from s in this.shards.Values select this.CreateReader(s, indexAccess)).ToArray<IndexReader>();
            return new IndexSearcher(this.CreateMultiReader(readers));
        }

        [Obsolete("Use CreateSearcher(LuceneIndexAccess)")]
        public IndexSearcher CreateSearcher(bool readOnly)
        {
            this.EnsureInitialized();
            if (this.IsSharded)
            {
                throw new InvalidOperationException("Not supported when index is sharded.");
            }
            return this.CreateSearcher(this.shards.First<KeyValuePair<int, LuceneShard>>().Value, readOnly ? LuceneIndexAccess.ReadOnlyCached : LuceneIndexAccess.ReadWrite);
        }

        public virtual IndexSearcher CreateSearcher(Shard shard, LuceneIndexAccess indexAccess)
        {
            LuceneShard shard2;
            Sitecore.Diagnostics.Assert.ArgumentNotNull(shard, "shard");
            this.EnsureInitialized();
            int id = shard.Id;
            if (this.IsSharded && (id < 0))
            {
                throw new ArgumentException("Invalid shard id: " + id, "shard");
            }
            if (!this.shards.TryGetValue(id, out shard2))
            {
                throw new ArgumentException("Invalid shard id: " + id, "shard");
            }
            return shard2.CreateSearcher(indexAccess);
        }

        public virtual Searcher CreateSearcher(IEnumerable<ILuceneProviderSearchable> searchables, LuceneIndexAccess indexAccess)
        {
            Sitecore.Diagnostics.Assert.ArgumentNotNull(searchables, "searchables");
            if (!searchables.Any<ILuceneProviderSearchable>())
            {
                throw new ArgumentException("searchables is empty.", "searchables");
            }
            this.EnsureInitialized();
            IndexReader[] readers = (from s in searchables select s.CreateReader(indexAccess)).ToArray<IndexReader>();
            return new IndexSearcher(this.CreateMultiReader(readers));
        }

        public override IProviderUpdateContext CreateUpdateContext()
        {
            this.EnsureInitialized();
            ICommitPolicyExecutor commitPolicyExecutor = (ICommitPolicyExecutor)this.CommitPolicyExecutor.Clone();
            commitPolicyExecutor.Initialize(this);
            return (this.lastUpdateContextCreated = new LuceneUpdateContext(this, commitPolicyExecutor));
        }

        [Obsolete("Use CreateWriter(Context, Shard, LuceneIndexMode)")]
        public IndexWriter CreateWriter(bool recreate)
        {
            this.EnsureInitialized();
            if (this.IsSharded)
            {
                throw new InvalidOperationException("Not supported when index is sharded.");
            }
            throw new InvalidOperationException("Not supported. Use CreateWriter(Context, Shard, LuceneIndexMode)");
        }

        public virtual IndexWriter CreateWriter(IProviderUpdateContext context, Shard shard, LuceneIndexMode mode)
        {
            this.EnsureInitialized();
            return this.shards[shard.Id].CreateWriter(context, mode);
        }

        public override void Delete(IIndexableId indexableId)
        {
            this.PerformDelete(indexableId, IndexingOptions.Default);
        }

        public override void Delete(IIndexableUniqueId indexableUniqueId)
        {
            this.PerformDelete(indexableUniqueId, IndexingOptions.Default);
        }

        public override void Delete(IIndexableId indexableId, IndexingOptions indexingOptions)
        {
            this.PerformDelete(indexableId, indexingOptions);
        }

        public override void Delete(IIndexableUniqueId indexableUniqueId, IndexingOptions indexingOptions)
        {
            this.PerformDelete(indexableUniqueId, indexingOptions);
        }

        protected override void Dispose(bool isDisposing)
        {
            if (!base.isDisposed)
            {
                base.Dispose(isDisposing);
                if (this.lastUpdateContextCreated != null)
                {
                    this.lastUpdateContextCreated.Dispose();
                    this.lastUpdateContextCreated = null;
                }
                TypeActionHelper.Call<IDisposable>(d => d.Dispose(), this.shards.Values);
                TypeActionHelper.Call<IDisposable>(d => d.Dispose(), new object[] { this.shardFactory });
                TypeActionHelper.Call<IDisposable>(d => d.Dispose(), this.strategies);
                TypeActionHelper.Call<IDisposable>(d => d.Dispose(), new object[] { this.PropertyStore });
                TypeActionHelper.Call<IDisposable>(d => d.Dispose(), new object[] { this.ShardFactory });
                TypeActionHelper.Call<IDisposable>(d => d.Dispose(), new object[] { this.CommitPolicyExecutor });
            }
        }

        [Obsolete("DoRebuild method is no longer in use and will be removed in later release. Use DoRebuild(IndexingOptions options, CancellationToken cancellationToken) instead.")]
        protected virtual void DoRebuild()
        {
            this.DoRebuild(IndexingOptions.Default, CancellationToken.None);
        }

        [Obsolete("DoRebuild method is no longer in use and will be removed in later release. Use DoRebuild(IndexingOptions options, CancellationToken cancellationToken) instead.")]
        protected virtual void DoRebuild(IndexingOptions indexingOptions)
        {
            this.DoRebuild(indexingOptions, CancellationToken.None);
        }

        [Obsolete("Use DoRebuild(IProviderUpdateContext, IndexingOptions, CancellationToken) instead.")]
        protected virtual void DoRebuild(IndexingOptions indexingOptions, CancellationToken cancellationToken)
        {
            base.VerifyNotDisposed();
            lock (this.GetFullRebuildLockObject())
            {
                using (IProviderUpdateContext context = this.CreateFullRebuildContext())
                {
                    this.DoRebuild(context, indexingOptions, cancellationToken);
                }
            }
        }

        protected virtual void DoRebuild(IProviderUpdateContext context, IndexingOptions indexingOptions, CancellationToken cancellationToken)
        {
            base.VerifyNotDisposed();
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            lock (this.GetFullRebuildLockObject())
            {
                foreach (IProviderCrawler crawler in base.Crawlers)
                {
                    crawler.RebuildFromRoot(context, indexingOptions, cancellationToken);
                }
                if ((base.IndexingState & IndexingState.Stopped) != IndexingState.Stopped)
                {
                    context.Optimize();
                }
                context.Commit();
                context.Optimize();
                stopwatch.Stop();
                if ((base.IndexingState & IndexingState.Stopped) != IndexingState.Stopped)
                {
                    this.PropertyStore.Set(IndexProperties.RebuildTime, stopwatch.ElapsedMilliseconds.ToString(CultureInfo.InvariantCulture));
                }
            }
        }

        protected virtual void DoReset(IProviderUpdateContext context)
        {
            base.VerifyNotDisposed();
            foreach (LuceneShard shard in this.shards.Values)
            {
                lock (this)
                {
                    shard.Reset();
                }
            }
        }

        protected void EnsureInitialized()
        {
            base.VerifyNotDisposed();
            Sitecore.Diagnostics.Assert.IsNotNull(this.Configuration, "Configuration");
            Sitecore.Diagnostics.Assert.IsTrue(this.Configuration is LuceneIndexConfiguration, "Configuration type is not expected.");
            if (!base.initialized)
            {
                throw new InvalidOperationException("Index has not been initialized.");
            }
        }

        public virtual string GetFolderPath(Shard shard)
        {
            if (this.FolderName == null)
            {
                return null;
            }
            string folderName = this.FolderName;
            string str2 = base.Locator.GetInstance<ISettings>().IndexFolder();
            if (this.IsSharded)
            {
                string str3;
                if ((this.shardRootFolderPaths != null) && this.shardRootFolderPaths.TryGetValue(shard.Name, out str3))
                {
                    str2 = Path.Combine(str2, str3);
                }
                folderName = this.FolderName + "_" + shard.Name;
            }
            return Sitecore.IO.FileUtil.MapPath(Path.Combine(str2, folderName));
        }

        protected virtual object GetFullRebuildLockObject() =>
            this.indexUpdateLock;

        public override void Initialize()
        {
            LuceneIndexConfiguration configuration = this.Configuration as LuceneIndexConfiguration;
            if (configuration == null)
            {
                throw new ConfigurationErrorsException("Index has no configuration.");
            }
            if (configuration.IndexDocumentPropertyMapper == null)
            {
                throw new ConfigurationErrorsException("IndexDocumentPropertyMapper have not been configured.");
            }
            if (configuration.IndexFieldStorageValueFormatter == null)
            {
                throw new ConfigurationErrorsException("IndexFieldStorageValueFormatter have not been configured.");
            }
            if (configuration.FieldReaders == null)
            {
                throw new ConfigurationErrorsException("FieldReaders have not been configured.");
            }
            if (this.PropertyStore == null)
            {
                throw new ConfigurationErrorsException("Index PropertyStore have not been configured.");
            }
            base.InitializeSearchIndexInitializables(new object[] { this.Configuration, base.Crawlers, this.strategies, this.ShardingStrategy });
            this.fieldNameTranslator = new LuceneFieldNameTranslator(this);
            if (this.CommitPolicyExecutor == null)
            {
                this.CommitPolicyExecutor = new NullCommitPolicyExecutor();
            }
            BooleanQuery.MaxClauseCount = this.settings.LuceneMaxClauseCount();
            this.isSharded = this.ShardingStrategy != null;
            base.initialized = true;
            this.InitializeShards();
            base.CheckInvalidConfiguration();
        }

        protected virtual void InitializeShards()
        {
            if (!this.IsSharded)
            {
                this.shards[Shard.NotSharded.Id] = (LuceneShard)this.ShardFactory.CreateNotShardedShard(this);
            }
            else
            {
                foreach (LuceneShard shard in this.ShardingStrategy.GetAllShards().Cast<LuceneShard>())
                {
                    this.shards[shard.Id] = shard;
                }
            }
            foreach (LuceneShard shard2 in this.shards.Values)
            {
                shard2.Initialize();
            }
        }

        private void PerformDelete(IIndexableId indexableId, IndexingOptions indexingOptions)
        {
            base.VerifyNotDisposed();
            if (base.ShouldStartIndexing(indexingOptions))
            {
                using (IProviderUpdateContext context = this.CreateUpdateContext())
                {
                    foreach (IProviderCrawler crawler in base.Crawlers)
                    {
                        crawler.Delete(context, indexableId, indexingOptions);
                    }
                    context.Commit();
                }
            }
        }

        private void PerformDelete(IIndexableUniqueId indexableUniqueId, IndexingOptions indexingOptions)
        {
            base.VerifyNotDisposed();
            if (base.ShouldStartIndexing(indexingOptions))
            {
                using (IProviderUpdateContext context = this.CreateUpdateContext())
                {
                    foreach (IProviderCrawler crawler in base.Crawlers)
                    {
                        crawler.Delete(context, indexableUniqueId, indexingOptions);
                    }
                    context.Commit();
                }
            }
        }

        protected override void PerformRebuild(IndexingOptions indexingOptions, CancellationToken cancellationToken)
        {
            base.VerifyNotDisposed();
            if (base.ShouldStartIndexing(indexingOptions))
            {
                lock (this.GetFullRebuildLockObject())
                {
                    using (IProviderUpdateContext context = this.CreateFullRebuildContext())
                    {
                        CrawlingLog.Log.Warn($"[Index={this.Name}] Reset Started", null);
                        this.DoReset(context);
                        CrawlingLog.Log.Warn($"[Index={this.Name}] Reset Ended", null);
                        CrawlingLog.Log.Warn($"[Index={this.Name}] Full Rebuild Started", null);
                        this.DoRebuild(context, indexingOptions, cancellationToken);
                        CrawlingLog.Log.Warn($"[Index={this.Name}] Full Rebuild Ended", null);
                    }
                }
            }
        }

        protected override void PerformRefresh(IIndexable indexableStartingPoint, IndexingOptions indexingOptions, CancellationToken cancellationToken)
        {
            base.VerifyNotDisposed();
            if (base.ShouldStartIndexing(indexingOptions))
            {
                lock (this.indexUpdateLock)
                {
                    if (base.Crawlers.Any<IProviderCrawler>(c => c.HasItemsToIndex()))
                    {
                        using (IProviderUpdateContext context = this.CreateUpdateContext())
                        {
                            foreach (IProviderCrawler crawler in base.Crawlers)
                            {
                                crawler.RefreshFromRoot(context, indexableStartingPoint, indexingOptions, cancellationToken);
                            }
                            context.Commit();
                            if ((base.IndexingState & IndexingState.Stopped) != IndexingState.Stopped)
                            {
                                context.Optimize();
                            }
                        }
                    }
                }
            }
        }

        private void PerformUpdate(IIndexableUniqueId indexableUniqueId, IndexingOptions indexingOptions)
        {
            if (base.ShouldStartIndexing(indexingOptions))
            {
                using (IProviderUpdateContext context = this.CreateUpdateContext())
                {
                    foreach (IProviderCrawler crawler in base.Crawlers)
                    {
                        crawler.Update(context, indexableUniqueId, indexingOptions);
                    }
                    context.Commit();
                }
            }
        }

        private void PerformUpdate(IEnumerable<IIndexableUniqueId> indexableUniqueIds, IndexingOptions indexingOptions)
        {
            if (base.ShouldStartIndexing(indexingOptions))
            {
                IEvent instance = base.Locator.GetInstance<IEvent>();
                instance.RaiseEvent("indexing:start", new object[] { this.Name, false });
                IndexingStartedEvent event3 = new IndexingStartedEvent
                {
                    IndexName = this.Name,
                    FullRebuild = false
                };
                base.Locator.GetInstance<IEventManager>().QueueEvent<IndexingStartedEvent>(event3);
                Action<IIndexableUniqueId> body = null;
                using (IProviderUpdateContext context = this.CreateUpdateContext())
                {
                    if (context.IsParallel)
                    {
                        if (body == null)
                        {
                            body = delegate (IIndexableUniqueId uniqueId) {
                                if (this.ShouldStartIndexing(indexingOptions))
                                {
                                    foreach (IProviderCrawler crawler in this.Crawlers)
                                    {
                                        crawler.Update(context, uniqueId, indexingOptions);
                                    }
                                }
                            };
                        }
                        Parallel.ForEach<IIndexableUniqueId>(indexableUniqueIds, context.ParallelOptions, body);
                        if (!base.ShouldStartIndexing(indexingOptions))
                        {
                            context.Commit();
                            return;
                        }
                    }
                    else
                    {
                        foreach (IIndexableUniqueId id in indexableUniqueIds)
                        {
                            if (!base.ShouldStartIndexing(indexingOptions))
                            {
                                context.Commit();
                                return;
                            }
                            foreach (IProviderCrawler crawler in base.Crawlers)
                            {
                                crawler.Update(context, id, indexingOptions);
                            }
                        }
                    }
                    context.Commit();
                }
                instance.RaiseEvent("indexing:end", new object[] { this.Name, false });
                IndexingFinishedEvent event4 = new IndexingFinishedEvent
                {
                    IndexName = this.Name,
                    FullRebuild = false
                };
                base.Locator.GetInstance<IEventManager>().QueueEvent<IndexingFinishedEvent>(event4);
            }
        }

        public override void Rebuild()
        {
            base.VerifyNotDisposed();
            this.PerformRebuild(IndexingOptions.Default, CancellationToken.None);
        }

        public override void Rebuild(IndexingOptions indexingOptions)
        {
            base.VerifyNotDisposed();
            this.PerformRebuild(indexingOptions, CancellationToken.None);
        }

        public override void Refresh(IIndexable indexableStartingPoint)
        {
            this.PerformRefresh(indexableStartingPoint, IndexingOptions.Default, CancellationToken.None);
        }

        public override void Refresh(IIndexable indexableStartingPoint, IndexingOptions indexingOptions)
        {
            this.PerformRefresh(indexableStartingPoint, indexingOptions, CancellationToken.None);
        }

        public override void Reset()
        {
            base.VerifyNotDisposed();
            lock (this.indexUpdateLock)
            {
                using (IProviderUpdateContext context = this.CreateUpdateContext())
                {
                    this.DoReset(context);
                }
            }
        }

        public virtual void ResetCachedReader()
        {
            lock (this)
            {
                foreach (LuceneShard shard in this.shards.Values)
                {
                    shard.ResetCachedReader();
                }
            }
        }

        public virtual void ResetCachedReader(Shard shard)
        {
            lock (this)
            {
                LuceneShard shard2;
                if (!this.shards.TryGetValue(shard.Id, out shard2))
                {
                    throw new ArgumentException("Invalid shard.");
                }
                shard2.ResetCachedReader();
            }
        }

        IndexSearcher ILuceneProviderSearchable.CreateSearcher(LuceneIndexAccess indexAccess)
        {
            this.EnsureInitialized();
            if (!this.IsSharded)
            {
                return this.CreateSearcher(this.shards.Values.First<LuceneShard>(), indexAccess);
            }
            IndexReader[] readers = (from s in this.shards.Values select this.CreateReader(s, indexAccess)).ToArray<IndexReader>();
            return new IndexSearcher(this.CreateMultiReader(readers));
        }

        IndexWriter ILuceneProviderSearchable.CreateWriter(IProviderUpdateContext context, LuceneIndexMode mode)
        {
            this.EnsureInitialized();
            if (this.IsSharded)
            {
                throw new InvalidOperationException("Sharded lucene index cannot create a single writer.");
            }
            return this.shards.Values.First<LuceneShard>().CreateWriter(context, mode);
        }

        public override void Update(IIndexableUniqueId indexableUniqueId)
        {
            this.PerformUpdate(indexableUniqueId, IndexingOptions.Default);
        }

        public override void Update(IEnumerable<IIndexableUniqueId> indexableUniqueIds)
        {
            this.PerformUpdate(indexableUniqueIds, IndexingOptions.Default);
        }

        public override void Update(IIndexableUniqueId indexableUniqueId, IndexingOptions indexingOptions)
        {
            this.PerformUpdate(indexableUniqueId, indexingOptions);
        }

        public override void Update(IEnumerable<IIndexableUniqueId> indexableUniqueIds, IndexingOptions indexingOptions)
        {
            this.PerformUpdate(indexableUniqueIds, indexingOptions);
        }

        public ICommitPolicyExecutor CommitPolicyExecutor { get; set; }

        public override ProviderIndexConfiguration Configuration { get; set; }

        public override bool EnableFieldLanguageFallback { get; set; }

        public override bool EnableItemLanguageFallback { get; set; }

        public override AbstractFieldNameTranslator FieldNameTranslator
        {
            get
            {
                return this.fieldNameTranslator;
            }
            set
            {
                if (value == null)
                {
                    throw new ArgumentNullException("value");
                }
                this.fieldNameTranslator = value;
            }
        }
        public string FolderName { get; protected set; }

        public Sitecore.ContentSearch.Sharding.HashRange HashRange =>
            Sitecore.ContentSearch.Sharding.HashRange.FullRange();

        public override bool IsSharded =>
            this.isSharded;

        public override string Name =>
            this.name;

        public override IIndexOperations Operations =>
            new LuceneIndexOperations(this);

        public override IIndexPropertyStore PropertyStore { get; set; }

        public override ISearchIndexSchema Schema =>
            this.schema;

        public override IShardFactory ShardFactory =>
            this.shardFactory;

        public override IShardingStrategy ShardingStrategy { get; set; }

        public override IEnumerable<Shard> Shards =>
            ((IEnumerable<Shard>)this.shards.Values.ToList<LuceneShard>());

        public override ISearchIndexSummary Summary =>
            this.summary;
    }
}