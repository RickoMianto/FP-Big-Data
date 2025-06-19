// Configuration
const CONFIG = {
    API_BASE_URL: 'http://localhost:5000/api', // Sesuaikan dengan Flask API server
    ITEMS_PER_PAGE: 12,
    DEBOUNCE_DELAY: 300,
    MAX_SUGGESTIONS: 5
};

// Global state
let currentPage = 1;
let currentQuery = '';
let currentSort = 'relevance';
let searchHistory = JSON.parse(localStorage.getItem('searchHistory') || '[]');
let currentProducts = [];
let totalPages = 1;

// DOM Elements
const elements = {
    searchInput: document.getElementById('searchInput'),
    searchBtn: document.getElementById('searchBtn'),
    searchSuggestions: document.getElementById('searchSuggestions'),
    quickTags: document.querySelectorAll('.quick-tag'),
    loadingContainer: document.getElementById('loadingContainer'),
    resultsSection: document.getElementById('resultsSection'),
    errorContainer: document.getElementById('errorContainer'),
    emptyState: document.getElementById('emptyState'),
    resultsTitle: document.getElementById('resultsTitle'),
    resultsCount: document.getElementById('resultsCount'),
    productsGrid: document.getElementById('productsGrid'),
    pagination: document.getElementById('pagination'),
    sortSelect: document.getElementById('sortSelect'),
    modalOverlay: document.getElementById('modalOverlay'),
    modalBody: document.getElementById('modalBody'),
    modalClose: document.getElementById('modalClose'),
    retryBtn: document.getElementById('retryBtn'),
    errorMessage: document.getElementById('errorMessage'),
    emptySearchTerm: document.getElementById('emptySearchTerm')
};

// Utility Functions
const utils = {
    debounce: (func, delay) => {
        let timeoutId;
        return (...args) => {
            clearTimeout(timeoutId);
            timeoutId = setTimeout(() => func.apply(null, args), delay);
        };
    },

    formatPrice: (price) => {
        if (typeof price !== 'number') return 'Rp 0';
        return new Intl.NumberFormat('id-ID', {
            style: 'currency',
            currency: 'IDR',
            minimumFractionDigits: 0
        }).format(price);
    },

    formatNumber: (num) => {
        if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
        if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
        return num.toString();
    },

    generateStars: (rating) => {
        const fullStars = Math.floor(rating);
        const hasHalfStar = rating % 1 >= 0.5;
        let starsHTML = '';
        
        for (let i = 0; i < fullStars; i++) {
            starsHTML += '<i class="fas fa-star star"></i>';
        }
        
        if (hasHalfStar) {
            starsHTML += '<i class="fas fa-star-half-alt star"></i>';
        }
        
        const emptyStars = 5 - fullStars - (hasHalfStar ? 1 : 0);
        for (let i = 0; i < emptyStars; i++) {
            starsHTML += '<i class="far fa-star star"></i>';
        }
        
        return starsHTML;
    },

    sanitizeHTML: (str) => {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    },

    showNotification: (message, type = 'info') => {
        // Simple notification system
        const notification = document.createElement('div');
        notification.className = `notification notification-${type}`;
        notification.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            padding: 1rem 2rem;
            background: ${type === 'error' ? '#f56565' : '#667eea'};
            color: white;
            border-radius: 8px;
            z-index: 9999;
            opacity: 0;
            transform: translateX(100px);
            transition: all 0.3s ease;
        `;
        notification.textContent = message;
        
        document.body.appendChild(notification);
        
        setTimeout(() => {
            notification.style.opacity = '1';
            notification.style.transform = 'translateX(0)';
        }, 100);
        
        setTimeout(() => {
            notification.style.opacity = '0';
            notification.style.transform = 'translateX(100px)';
            setTimeout(() => notification.remove(), 300);
        }, 3000);
    }
};

// API Functions
const api = {
    async search(query, page = 1, sort = 'relevance') {
        try {
            const params = new URLSearchParams({
                q: query,
                page: page,
                per_page: CONFIG.ITEMS_PER_PAGE,
                sort: sort
            });

            const response = await fetch(`${CONFIG.API_BASE_URL}/search?${params}`);
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const data = await response.json();
            return data;
        } catch (error) {
            console.error('Search API error:', error);
            throw error;
        }
    },

    async getRecommendations(productId) {
        try {
            const response = await fetch(`${CONFIG.API_BASE_URL}/recommendations/${productId}`);
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const data = await response.json();
            return data;
        } catch (error) {
            console.error('Recommendations API error:', error);
            throw error;
        }
    },

    async getSuggestions(query) {
        try {
            const params = new URLSearchParams({
                q: query,
                limit: CONFIG.MAX_SUGGESTIONS
            });

            const response = await fetch(`${CONFIG.API_BASE_URL}/suggestions?${params}`);
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const data = await response.json();
            return data;
        } catch (error) {
            console.error('Suggestions API error:', error);
            return { suggestions: [] };
        }
    },

    async getProductDetails(productId) {
        try {
            const response = await fetch(`${CONFIG.API_BASE_URL}/product/${productId}`);
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const data = await response.json();
            return data;
        } catch (error) {
            console.error('Product details API error:', error);
            throw error;
        }
    }
};

// UI State Management
const ui = {
    showLoading() {
        elements.loadingContainer.style.display = 'flex';
        elements.resultsSection.style.display = 'none';
        elements.errorContainer.style.display = 'none';
        elements.emptyState.style.display = 'none';
    },

    hideLoading() {
        elements.loadingContainer.style.display = 'none';
    },

    showResults() {
        this.hideLoading();
        elements.resultsSection.style.display = 'block';
        elements.errorContainer.style.display = 'none';
        elements.emptyState.style.display = 'none';
    },

    showError(message) {
        this.hideLoading();
        elements.resultsSection.style.display = 'none';
        elements.errorContainer.style.display = 'block';
        elements.emptyState.style.display = 'none';
        elements.errorMessage.textContent = message;
    },

    showEmpty(searchTerm) {
        this.hideLoading();
        elements.resultsSection.style.display = 'none';
        elements.errorContainer.style.display = 'none';
        elements.emptyState.style.display = 'block';
        elements.emptySearchTerm.textContent = searchTerm;
    },

    updateResultsHeader(query, count, page, totalPages) {
        elements.resultsTitle.textContent = `Hasil Pencarian: "${query}"`;
        const start = (page - 1) * CONFIG.ITEMS_PER_PAGE + 1;
        const end = Math.min(page * CONFIG.ITEMS_PER_PAGE, count);
        elements.resultsCount.textContent = `Menampilkan ${start}-${end} dari ${count} produk`;
    },

    renderProducts(products) {
        const grid = elements.productsGrid;
        grid.innerHTML = '';

        products.forEach((product, index) => {
            const productCard = this.createProductCard(product);
            productCard.style.animationDelay = `${index * 0.1}s`;
            productCard.classList.add('fade-in');
            grid.appendChild(productCard);
        });
    },

    createProductCard(product) {
        const card = document.createElement('div');
        card.className = 'product-card';
        card.setAttribute('data-product-id', product.id || product.product_id || '');

        // Generate mock data if not provided
        const price = product.price || Math.floor(Math.random() * 1000000) + 50000;
        const rating = product.rating || (Math.random() * 2 + 3); // Rating 3-5
        const reviewCount = product.review_count || Math.floor(Math.random() * 500) + 10;
        const category = product.category || product.category_code || 'Elektronik';
        const imageUrl = product.image_url || null;

        card.innerHTML = `
            <div class="product-image">
                ${imageUrl ? 
                    `<img src="${imageUrl}" alt="${utils.sanitizeHTML(product.brand || product.product_name || 'Product')}" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';">
                     <div class="placeholder-icon" style="display: none;"><i class="fas fa-shopping-bag"></i></div>` :
                    `<div class="placeholder-icon"><i class="fas fa-shopping-bag"></i></div>`
                }
            </div>
            <div class="product-info">
                <div class="product-category">${utils.sanitizeHTML(category)}</div>
                <h3 class="product-title">${utils.sanitizeHTML(product.brand || product.product_name || 'Produk Tanpa Nama')}</h3>
                <div class="product-price">${utils.formatPrice(price)}</div>
                <div class="product-rating">
                    <div class="stars">${utils.generateStars(rating)}</div>
                    <span class="rating-text">${rating.toFixed(1)} (${utils.formatNumber(reviewCount)})</span>
                </div>
                <div class="product-stats">
                    <span>Event: ${utils.sanitizeHTML(product.event_type || 'view')}</span>
                    <span>ID: ${utils.sanitizeHTML(product.id || product.product_id || 'N/A')}</span>
                </div>
            </div>
        `;

        card.addEventListener('click', () => this.showProductModal(product));
        return card;
    },

    renderPagination(currentPage, totalPages) {
        const pagination = elements.pagination;
        pagination.innerHTML = '';

        if (totalPages <= 1) return;

        // Previous button
        const prevBtn = document.createElement('button');
        prevBtn.className = 'page-btn';
        prevBtn.innerHTML = '<i class="fas fa-chevron-left"></i> Sebelumnya';
        prevBtn.disabled = currentPage === 1;
        prevBtn.addEventListener('click', () => {
            if (currentPage > 1) search.performSearch(currentQuery, currentPage - 1, currentSort);
        });
        pagination.appendChild(prevBtn);

        // Page numbers
        const startPage = Math.max(1, currentPage - 2);
        const endPage = Math.min(totalPages, currentPage + 2);

        if (startPage > 1) {
            const firstBtn = document.createElement('button');
            firstBtn.className = 'page-btn';
            firstBtn.textContent = '1';
            firstBtn.addEventListener('click', () => search.performSearch(currentQuery, 1, currentSort));
            pagination.appendChild(firstBtn);

            if (startPage > 2) {
                const ellipsis = document.createElement('span');
                ellipsis.textContent = '...';
                ellipsis.style.padding = '0.75rem';
                pagination.appendChild(ellipsis);
            }
        }

        for (let i = startPage; i <= endPage; i++) {
            const pageBtn = document.createElement('button');
            pageBtn.className = `page-btn ${i === currentPage ? 'active' : ''}`;
            pageBtn.textContent = i;
            pageBtn.addEventListener('click', () => search.performSearch(currentQuery, i, currentSort));
            pagination.appendChild(pageBtn);
        }

        if (endPage < totalPages) {
            if (endPage < totalPages - 1) {
                const ellipsis = document.createElement('span');
                ellipsis.textContent = '...';
                ellipsis.style.padding = '0.75rem';
                pagination.appendChild(ellipsis);
            }

            const lastBtn = document.createElement('button');
            lastBtn.className = 'page-btn';
            lastBtn.textContent = totalPages;
            lastBtn.addEventListener('click', () => search.performSearch(currentQuery, totalPages, currentSort));
            pagination.appendChild(lastBtn);
        }

        // Next button
        const nextBtn = document.createElement('button');
        nextBtn.className = 'page-btn';
        nextBtn.innerHTML = 'Selanjutnya <i class="fas fa-chevron-right"></i>';
        nextBtn.disabled = currentPage === totalPages;
        nextBtn.addEventListener('click', () => {
            if (currentPage < totalPages) search.performSearch(currentQuery, currentPage + 1, currentSort);
        });
        pagination.appendChild(nextBtn);
    },

    async showProductModal(product) {
        const modal = elements.modalOverlay;
        const modalBody = elements.modalBody;

        // Show loading in modal
        modalBody.innerHTML = '<div class="text-center"><div class="spinner"></div><p>Memuat detail produk...</p></div>';
        modal.style.display = 'flex';

        try {
            // Try to get detailed product info
            let productDetails = product;
            if (product.id || product.product_id) {
                try {
                    const details = await api.getProductDetails(product.id || product.product_id);
                    productDetails = { ...product, ...details };
                } catch (error) {
                    console.warn('Could not fetch product details:', error);
                }
            }

            // Get recommendations
            let recommendations = [];
            if (product.id || product.product_id) {
                try {
                    const recData = await api.getRecommendations(product.id || product.product_id);
                    recommendations = recData.recommendations || [];
                } catch (error) {
                    console.warn('Could not fetch recommendations:', error);
                }
            }

            this.renderProductModal(productDetails, recommendations);
        } catch (error) {
            modalBody.innerHTML = `
                <div class="text-center">
                    <i class="fas fa-exclamation-triangle" style="font-size: 3rem; color: #ff6b6b; margin-bottom: 1rem;"></i>
                    <h3>Gagal Memuat Detail</h3>
                    <p>Tidak dapat memuat detail produk. Silakan coba lagi.</p>
                </div>
            `;
        }
    },

    renderProductModal(product, recommendations = []) {
        const modalBody = elements.modalBody;
        const price = product.price || Math.floor(Math.random() * 1000000) + 50000;
        const rating = product.rating || (Math.random() * 2 + 3);
        const reviewCount = product.review_count || Math.floor(Math.random() * 500) + 10;

        modalBody.innerHTML = `
            <div class="modal-product-details">
                <div class="modal-product-header">
                    <div class="modal-product-image">
                        ${product.image_url ? 
                            `<img src="${product.image_url}" alt="${utils.sanitizeHTML(product.brand || product.product_name)}" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';">
                             <div class="placeholder-icon" style="display: none;"><i class="fas fa-shopping-bag"></i></div>` :
                            `<div class="placeholder-icon"><i class="fas fa-shopping-bag"></i></div>`
                        }
                    </div>
                    <div class="modal-product-info">
                        <div class="product-category">${utils.sanitizeHTML(product.category || product.category_code || 'Elektronik')}</div>
                        <h2>${utils.sanitizeHTML(product.brand || product.product_name || 'Produk Tanpa Nama')}</h2>
                        <div class="product-price" style="font-size: 2rem; margin: 1rem 0;">${utils.formatPrice(price)}</div>
                        <div class="product-rating" style="margin-bottom: 1rem;">
                            <div class="stars">${utils.generateStars(rating)}</div>
                            <span class="rating-text">${rating.toFixed(1)} dari 5 (${utils.formatNumber(reviewCount)} ulasan)</span>
                        </div>
                        <div class="product-description">
                            <p>${product.description || 'Deskripsi produk tidak tersedia. Produk ini adalah bagian dari katalog e-commerce yang menampilkan berbagai pilihan barang berkualitas dengan harga terjangkau.'}</p>
                        </div>
                        <div class="product-meta" style="margin-top: 1rem; padding-top: 1rem; border-top: 1px solid #f0f0f0;">
                            <p><strong>ID Produk:</strong> ${product.id || product.product_id || 'N/A'}</p>
                            <p><strong>Event Type:</strong> ${product.event_type || 'view'}</p>
                            ${product.user_id ? `<p><strong>User ID:</strong> ${product.user_id}</p>` : ''}
                        </div>
                    </div>
                </div>
                
                ${recommendations.length > 0 ? `
                    <div class="modal-recommendations" style="margin-top: 2rem; padding-top: 2rem; border-top: 2px solid #f0f0f0;">
                        <h3 style="margin-bottom: 1rem;">Produk Rekomendasi</h3>
                        <div class="recommendations-grid" style="display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 1rem;">
                            ${recommendations.slice(0, 4).map(rec => `
                                <div class="recommendation-item" style="border: 1px solid #f0f0f0; border-radius: 8px; padding: 1rem; text-align: center; cursor: pointer;" data-product-id="${rec.id || rec.product_id}">
                                    <div class="rec-image" style="width: 60px; height: 60px; background: #f8f9fa; border-radius: 8px; margin: 0 auto 0.5rem; display: flex; align-items: center; justify-content: center;">
                                        <i class="fas fa-shopping-bag" style="color: #ccc;"></i>
                                    </div>
                                    <div class="rec-title" style="font-size: 0.9rem; font-weight: 500; margin-bottom: 0.5rem;">${utils.sanitizeHTML(rec.brand || rec.product_name || 'Produk')}</div>
                                    <div class="rec-price" style="color: #667eea; font-weight: 600;">${utils.formatPrice(rec.price || Math.floor(Math.random() * 500000) + 25000)}</div>
                                </div>
                            `).join('')}
                        </div>
                    </div>
                ` : ''}
            </div>
        `;

        // Add click handlers for recommendations
        modalBody.querySelectorAll('.recommendation-item').forEach(item => {
            item.addEventListener('click', async () => {
                const productId = item.dataset.productId;
                const recProduct = recommendations.find(r => (r.id || r.product_id) === productId);
                if (recProduct) {
                    await this.showProductModal(recProduct);
                }
            });
        });
    },

    hideModal() {
        elements.modalOverlay.style.display = 'none';
    },

    showSuggestions(suggestions) {
        const container = elements.searchSuggestions;
        
        if (suggestions.length === 0) {
            container.style.display = 'none';
            return;
        }

        container.innerHTML = suggestions.map(suggestion => 
            `<div class="suggestion-item" data-suggestion="${utils.sanitizeHTML(suggestion)}">${utils.sanitizeHTML(suggestion)}</div>`
        ).join('');
        
        container.style.display = 'block';

        // Add click handlers
        container.querySelectorAll('.suggestion-item').forEach(item => {
            item.addEventListener('click', () => {
                const suggestion = item.dataset.suggestion;
                elements.searchInput.value = suggestion;
                container.style.display = 'none';
                search.performSearch(suggestion);
            });
        });
    },

    hideSuggestions() {
        elements.searchSuggestions.style.display = 'none';
    }
};

// Search functionality
const search = {
    async performSearch(query, page = 1, sort = 'relevance') {
        if (!query.trim()) {
            utils.showNotification('Silakan masukkan kata kunci pencarian', 'error');
            return;
        }

        // Update global state
        currentQuery = query.trim();
        currentPage = page;
        currentSort = sort;

        // Add to search history
        this.addToHistory(currentQuery);

        // Show loading
        ui.showLoading();

        // Scroll to results section
        if (page === 1) {
            document.querySelector('.hero').scrollIntoView({ behavior: 'smooth' });
        }

        try {
            const data = await api.search(currentQuery, currentPage, currentSort);
            
            if (data.success) {
                currentProducts = data.products || [];
                totalPages = Math.ceil((data.total || 0) / CONFIG.ITEMS_PER_PAGE);

                if (currentProducts.length === 0) {
                    ui.showEmpty(currentQuery);
                } else {
                    ui.showResults();
                    ui.updateResultsHeader(currentQuery, data.total || 0, currentPage, totalPages);
                    ui.renderProducts(currentProducts);
                    ui.renderPagination(currentPage, totalPages);
                }
            } else {
                throw new Error(data.message || 'Search failed');
            }
        } catch (error) {
            console.error('Search error:', error);
            ui.showError('Gagal melakukan pencarian. Silakan periksa koneksi internet dan coba lagi.');
            utils.showNotification('Pencarian gagal. Silakan coba lagi.', 'error');
        }
    },

    addToHistory(query) {
        // Remove if already exists
        searchHistory = searchHistory.filter(item => item !== query);
        // Add to beginning
        searchHistory.unshift(query);
        // Keep only last 10 searches
        searchHistory = searchHistory.slice(0, 10);
        // Save to localStorage
        localStorage.setItem('searchHistory', JSON.stringify(searchHistory));
    },

    async getSuggestions(query) {
        if (!query.trim() || query.length < 2) {
            ui.hideSuggestions();
            return;
        }

        try {
            // Combine API suggestions with search history
            const apiData = await api.getSuggestions(query);
            const apiSuggestions = apiData.suggestions || [];
            
            // Filter search history based on query
            const historySuggestions = searchHistory.filter(item => 
                item.toLowerCase().includes(query.toLowerCase())
            );

            // Combine and deduplicate
            const allSuggestions = [...new Set([...apiSuggestions, ...historySuggestions])];
            const limitedSuggestions = allSuggestions.slice(0, CONFIG.MAX_SUGGESTIONS);

            ui.showSuggestions(limitedSuggestions);
        } catch (error) {
            console.error('Suggestions error:', error);
            // Fallback to search history only
            const historySuggestions = searchHistory.filter(item => 
                item.toLowerCase().includes(query.toLowerCase())
            ).slice(0, CONFIG.MAX_SUGGESTIONS);
            ui.showSuggestions(historySuggestions);
        }
    }
};

// Event Listeners
const initEventListeners = () => {
    // Search input events
    elements.searchInput.addEventListener('input', utils.debounce((e) => {
        const query = e.target.value.trim();
        search.getSuggestions(query);
    }, CONFIG.DEBOUNCE_DELAY));

    elements.searchInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            e.preventDefault();
            ui.hideSuggestions();
            search.performSearch(elements.searchInput.value);
        }
    });

    // Search button
    elements.searchBtn.addEventListener('click', () => {
        ui.hideSuggestions();
        search.performSearch(elements.searchInput.value);
    });

    // Quick tags
    elements.quickTags.forEach(tag => {
        tag.addEventListener('click', () => {
            const searchTerm = tag.dataset.search;
            elements.searchInput.value = searchTerm;
            search.performSearch(searchTerm);
        });
    });

    // Sort select
    elements.sortSelect.addEventListener('change', (e) => {
        if (currentQuery) {
            search.performSearch(currentQuery, 1, e.target.value);
        }
    });

    // Modal close
    elements.modalClose.addEventListener('click', ui.hideModal);
    elements.modalOverlay.addEventListener('click', (e) => {
        if (e.target === elements.modalOverlay) {
            ui.hideModal();
        }
    });

    // Retry button
    elements.retryBtn.addEventListener('click', () => {
        if (currentQuery) {
            search.performSearch(currentQuery, currentPage, currentSort);
        }
    });

    // Click outside to hide suggestions
    document.addEventListener('click', (e) => {
        if (!elements.searchInput.contains(e.target) && !elements.searchSuggestions.contains(e.target)) {
            ui.hideSuggestions();
        }
    });

    // Escape key to close modal and suggestions
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
            ui.hideModal();
            ui.hideSuggestions();
        }
    });

    // Smooth scrolling for navigation
    document.querySelectorAll('.nav-link').forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            const targetId = link.getAttribute('href');
            if (targetId.startsWith('#')) {
                const targetElement = document.querySelector(targetId);
                if (targetElement) {
                    targetElement.scrollIntoView({ behavior: 'smooth' });
                }
            }
        });
    });
};

// Demo data for testing when API is not available
const demoData = {
    generateDemoProducts: (query, count = 12) => {
        const categories = ['smartphone', 'laptop', 'headphone', 'mouse', 'keyboard', 'tablet', 'speaker', 'camera'];
        const brands = ['Samsung', 'Apple', 'Xiaomi', 'Asus', 'Dell', 'HP', 'Logitech', 'Sony', 'Canon', 'JBL'];
        const events = ['view', 'cart', 'purchase'];
        
        return Array.from({ length: count }, (_, i) => ({
            id: `demo_${Date.now()}_${i}`,
            product_id: `prod_${i + 1}`,
            product_name: `${brands[Math.floor(Math.random() * brands.length)]} ${query} ${i + 1}`,
            brand: brands[Math.floor(Math.random() * brands.length)],
            category: categories[Math.floor(Math.random() * categories.length)],
            category_code: categories[Math.floor(Math.random() * categories.length)],
            price: Math.floor(Math.random() * 2000000) + 100000,
            rating: Math.random() * 2 + 3,
            review_count: Math.floor(Math.random() * 1000) + 10,
            event_type: events[Math.floor(Math.random() * events.length)],
            user_id: `user_${Math.floor(Math.random() * 1000)}`,
            description: `Produk ${query} berkualitas tinggi dengan fitur terdepan dan desain modern yang cocok untuk kebutuhan sehari-hari.`
        }));
    },

    async searchDemo(query, page = 1) {
        // Simulate API delay
        await new Promise(resolve => setTimeout(resolve, 800));
        
        const products = this.generateDemoProducts(query, CONFIG.ITEMS_PER_PAGE);
        const total = Math.floor(Math.random() * 200) + 50;
        
        return {
            success: true,
            products: products,
            total: total,
            page: page,
            per_page: CONFIG.ITEMS_PER_PAGE
        };
    }
};

// Fallback to demo data when API is not available
const searchWithFallback = async (query, page = 1, sort = 'relevance') => {
    try {
        return await api.search(query, page, sort);
    } catch (error) {
        console.warn('API not available, using demo data:', error);
        utils.showNotification('Menggunakan data demo - API tidak tersedia', 'info');
        return await demoData.searchDemo(query, page);
    }
};

// Override API search with fallback
api.search = searchWithFallback;

// Initialize app
const init = () => {
    console.log('E-commerce Recommendation System initialized');
    initEventListeners();
    
    // Check if there's a search query in URL
    const urlParams = new URLSearchParams(window.location.search);
    const query = urlParams.get('q');
    if (query) {
        elements.searchInput.value = query;
        search.performSearch(query);
    }
    
    // Add some example searches to history if empty
    if (searchHistory.length === 0) {
        searchHistory = ['smartphone', 'laptop', 'headphone', 'mouse gaming', 'keyboard mechanical'];
        localStorage.setItem('searchHistory', JSON.stringify(searchHistory));
    }
};

// Wait for DOM to be ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
} else {
    init();
}