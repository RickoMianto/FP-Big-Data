// API Configuration
const API_BASE_URL = "http://localhost:5000/api";

// Global State
let currentPage = 1;
let itemsPerPage = 12;
let currentFilters = {
	search: "",
	category: "",
	brand: "",
	priceRange: "",
};
let allProducts = [];
let filteredProducts = [];

// DOM Elements
const searchInput = document.getElementById("searchInput");
const searchBtn = document.getElementById("searchBtn");
const categoryFilter = document.getElementById("categoryFilter");
const brandFilter = document.getElementById("brandFilter");
const priceFilter = document.getElementById("priceFilter");
const productsGrid = document.getElementById("productsGrid");
const loading = document.getElementById("loading");
const pagination = document.getElementById("pagination");
const prevBtn = document.getElementById("prevBtn");
const nextBtn = document.getElementById("nextBtn");
const pageInfo = document.getElementById("pageInfo");
const productModal = document.getElementById("productModal");

// Initialize the application
document.addEventListener("DOMContentLoaded", function () {
	initializeApp();
	setupEventListeners();
	loadInitialData();
});

// Initialize application
function initializeApp() {
	// Smooth scrolling for navigation
	document.querySelectorAll(".nav-link").forEach((link) => {
		link.addEventListener("click", function (e) {
			e.preventDefault();
			const targetId = this.getAttribute("href").substring(1);
			scrollToSection(targetId);

			// Update active navigation
			document.querySelectorAll(".nav-link").forEach((l) => l.classList.remove("active"));
			this.classList.add("active");
		});
	});
}

// Setup event listeners
function setupEventListeners() {
	// Search functionality
	searchBtn.addEventListener("click", performSearch);
	searchInput.addEventListener("keypress", function (e) {
		if (e.key === "Enter") {
			performSearch();
		}
	});

	// Filter functionality
	categoryFilter.addEventListener("change", applyFilters);
	brandFilter.addEventListener("change", applyFilters);
	priceFilter.addEventListener("change", applyFilters);

	// Pagination
	prevBtn.addEventListener("click", () => changePage(currentPage - 1));
	nextBtn.addEventListener("click", () => changePage(currentPage + 1));

	// Modal close
	window.addEventListener("click", function (e) {
		if (e.target === productModal) {
			closeModal();
		}
	});
}

// Load initial data
async function loadInitialData() {
	try {
		showLoading(true);

		// Load products
		await loadProducts();

		// Load filter options
		await loadFilterOptions();

		// Load analytics
		await loadAllAnalytics();

		showLoading(false);
	} catch (error) {
		console.error("Error loading initial data:", error);
		showError("Failed to load data. Please try again.");
		showLoading(false);
	}
}

// Load products from API
async function loadProducts() {
	try {
		const response = await fetch(`${API_BASE_URL}/products`);
		if (!response.ok) {
			throw new Error("Failed to fetch products");
		}

		const data = await response.json();
		allProducts = data.products || [];
		filteredProducts = [...allProducts];

		renderProducts();
		updatePagination();
	} catch (error) {
		console.error("Error loading products:", error);
		// Use mock data for demonstration
		loadMockProducts();
	}
}

// Load mock products for demonstration
function loadMockProducts() {
	allProducts = [
		{
			product_id: "1003",
			category_code: "construction.tools.light",
			brand: "Samsung",
			price: 1302.48,
			view_count: 1250,
			cart_count: 95,
			purchase_count: 43,
		},
		{
			product_id: "1015",
			category_code: "appliances.personal.massager",
			brand: "bosch",
			price: 313.52,
			view_count: 890,
			cart_count: 67,
			purchase_count: 23,
		},
		{
			product_id: "1009",
			category_code: "apparel.trousers",
			brand: "Balenciaga",
			price: 101.68,
			view_count: 2100,
			cart_count: 156,
			purchase_count: 89,
		},
	];

	filteredProducts = [...allProducts];
	renderProducts();
	updatePagination();
}

// Load filter options
async function loadFilterOptions() {
	try {
		// Extract unique categories and brands from products
		const categories = [...new Set(allProducts.map((p) => p.category_code))].filter(Boolean);
		const brands = [...new Set(allProducts.map((p) => p.brand))].filter(Boolean);

		// Populate category filter
		categoryFilter.innerHTML = '<option value="">All Categories</option>';
		categories.forEach((category) => {
			const option = document.createElement("option");
			option.value = category;
			option.textContent = formatCategoryName(category);
			categoryFilter.appendChild(option);
		});

		// Populate brand filter
		brandFilter.innerHTML = '<option value="">All Brands</option>';
		brands.forEach((brand) => {
			const option = document.createElement("option");
			option.value = brand;
			option.textContent = capitalizeFirst(brand);
			brandFilter.appendChild(option);
		});
	} catch (error) {
		console.error("Error loading filter options:", error);
	}
}

// Perform search
function performSearch() {
	currentFilters.search = searchInput.value.trim();
	applyFilters();
}

// Apply filters
function applyFilters() {
	currentFilters.category = categoryFilter.value;
	currentFilters.brand = brandFilter.value;
	currentFilters.priceRange = priceFilter.value;

	filteredProducts = allProducts.filter((product) => {
		// Search filter
		if (currentFilters.search) {
			const searchTerm = currentFilters.search.toLowerCase();
			const productText = `${product.category_code} ${product.brand}`.toLowerCase();
			if (!productText.includes(searchTerm)) {
				return false;
			}
		}

		// Category filter
		if (currentFilters.category && product.category_code !== currentFilters.category) {
			return false;
		}

		// Brand filter
		if (currentFilters.brand && product.brand !== currentFilters.brand) {
			return false;
		}

		// Price filter
		if (currentFilters.priceRange) {
			const price = parseFloat(product.price);
			const [min, max] = currentFilters.priceRange.split("-").map((p) => (p === "" ? Infinity : parseFloat(p.replace("+", ""))));
			if (price < min || (max !== Infinity && price > max)) {
				return false;
			}
		}

		return true;
	});

	currentPage = 1;
	renderProducts();
	updatePagination();
}

// Render products
function renderProducts() {
	const startIndex = (currentPage - 1) * itemsPerPage;
	const endIndex = startIndex + itemsPerPage;
	const productsToShow = filteredProducts.slice(startIndex, endIndex);

	if (productsToShow.length === 0) {
		productsGrid.innerHTML = `
            <div class="no-products">
                <i class="fas fa-search" style="font-size: 3rem; color: #ccc; margin-bottom: 1rem;"></i>
                <h3>No products found</h3>
                <p>Try adjusting your search or filters</p>
            </div>
        `;
		return;
	}

	productsGrid.innerHTML = productsToShow
		.map(
			(product) => `
        <div class="product-card" onclick="showProductDetails('${product.product_id}')">
            <div class="product-image">
                <i class="fas fa-box"></i>
            </div>
            <div class="product-info">
                <div class="product-title">Product ${product.product_id}</div>
                <div class="product-category">${formatCategoryName(product.category_code)}</div>
                <div class="product-brand">Brand: ${capitalizeFirst(product.brand)}</div>
                <div class="product-price">$${parseFloat(product.price).toFixed(2)}</div>
                <div class="product-stats">
                    <div class="stat">
                        <span class="stat-value">${product.view_count || 0}</span>
                        <span class="stat-label">Views</span>
                    </div>
                    <div class="stat">
                        <span class="stat-value">${product.cart_count || 0}</span>
                        <span class="stat-label">Cart</span>
                    </div>
                    <div class="stat">
                        <span class="stat-value">${product.purchase_count || 0}</span>
                        <span class="stat-label">Purchases</span>
                    </div>
                </div>
            </div>
        </div>
    `
		)
		.join("");
}

// Update pagination
function updatePagination() {
	const totalPages = Math.ceil(filteredProducts.length / itemsPerPage);

	if (totalPages <= 1) {
		pagination.classList.add("hidden");
		return;
	}

	pagination.classList.remove("hidden");
	pageInfo.textContent = `Page ${currentPage} of ${totalPages}`;

	prevBtn.disabled = currentPage === 1;
	nextBtn.disabled = currentPage === totalPages;
}

// Change page
function changePage(newPage) {
	const totalPages = Math.ceil(filteredProducts.length / itemsPerPage);

	if (newPage >= 1 && newPage <= totalPages) {
		currentPage = newPage;
		renderProducts();
		updatePagination();
		scrollToSection("products");
	}
}

// Show product details modal
function showProductDetails(productId) {
	const product = allProducts.find((p) => p.product_id === productId);
	if (!product) return;

	const modalBody = document.getElementById("modalBody");
	modalBody.innerHTML = `
        <div class="product-details">
            <div class="product-image-large">
                <i class="fas fa-box" style="font-size: 4rem; color: #667eea;"></i>
            </div>
            <h2>Product ${product.product_id}</h2>
            <div class="product-detail-grid">
                <div class="detail-item">
                    <strong>Category:</strong>
                    <span>${formatCategoryName(product.category_code)}</span>
                </div>
                <div class="detail-item">
                    <strong>Brand:</strong>
                    <span>${capitalizeFirst(product.brand)}</span>
                </div>
                <div class="detail-item">
                    <strong>Price:</strong>
                    <span class="price-highlight">${parseFloat(product.price).toFixed(2)}</span>
                </div>
            </div>
            <div class="analytics-summary">
                <h3>Product Analytics</h3>
                <div class="stats-grid">
                    <div class="stat-card">
                        <i class="fas fa-eye"></i>
                        <div class="stat-number">${product.view_count || 0}</div>
                        <div class="stat-label">Total Views</div>
                    </div>
                    <div class="stat-card">
                        <i class="fas fa-shopping-cart"></i>
                        <div class="stat-number">${product.cart_count || 0}</div>
                        <div class="stat-label">Added to Cart</div>
                    </div>
                    <div class="stat-card">
                        <i class="fas fa-credit-card"></i>
                        <div class="stat-number">${product.purchase_count || 0}</div>
                        <div class="stat-label">Purchases</div>
                    </div>
                </div>
                <div class="recommendation-score">
                    <h4>AI Recommendation Score</h4>
                    <div class="score-bar">
                        <div class="score-fill" style="width: ${calculateRecommendationScore(product)}%"></div>
                    </div>
                    <span class="score-text">${calculateRecommendationScore(product)}% Match</span>
                </div>
            </div>
        </div>
    `;

	productModal.classList.remove("hidden");
}

// Close modal
function closeModal() {
	productModal.classList.add("hidden");
}

// Load all analytics
async function loadAllAnalytics() {
	await Promise.all([loadTopProducts("view"), loadTopProducts("cart"), loadTopProducts("purchase")]);
}

// Load top products for analytics
async function loadTopProducts(type) {
	try {
		const response = await fetch(`${API_BASE_URL}/analytics/top-${type}`);
		let topProducts;

		if (response.ok) {
			const data = await response.json();
			topProducts = data.products || [];
		} else {
			// Use mock data
			topProducts = getMockTopProducts(type);
		}

		renderTopProducts(type, topProducts);
	} catch (error) {
		console.error(`Error loading top ${type} products:`, error);
		renderTopProducts(type, getMockTopProducts(type));
	}
}

// Get mock top products
function getMockTopProducts(type) {
	const sortedProducts = [...allProducts].sort((a, b) => {
		const aCount = a[`${type}_count`] || 0;
		const bCount = b[`${type}_count`] || 0;
		return bCount - aCount;
	});

	return sortedProducts.slice(0, 5);
}

// Render top products
function renderTopProducts(type, products) {
	const containerId = type === "view" ? "topViewed" : type === "cart" ? "topCart" : "topPurchased";
	const container = document.getElementById(containerId);

	if (!products || products.length === 0) {
		container.innerHTML = '<p class="no-data">No data available</p>';
		return;
	}

	container.innerHTML = products
		.map(
			(product, index) => `
        <div class="top-product-item" onclick="showProductDetails('${product.product_id}')">
            <div class="product-rank">${index + 1}</div>
            <div class="top-product-info">
                <div class="top-product-name">Product ${product.product_id}</div>
                <div class="top-product-category">${formatCategoryName(product.category_code)}</div>
            </div>
            <div class="top-product-count">${product[`${type}_count`] || 0}</div>
        </div>
    `
		)
		.join("");
}

// Utility Functions
function showLoading(show) {
	if (show) {
		loading.classList.remove("hidden");
		productsGrid.classList.add("hidden");
	} else {
		loading.classList.add("hidden");
		productsGrid.classList.remove("hidden");
	}
}

function showError(message) {
	productsGrid.innerHTML = `
        <div class="error-message">
            <i class="fas fa-exclamation-triangle" style="font-size: 3rem; color: #ff6b6b; margin-bottom: 1rem;"></i>
            <h3>Error</h3>
            <p>${message}</p>
            <button onclick="loadInitialData()" class="retry-btn">Retry</button>
        </div>
    `;
}

function scrollToSection(sectionId) {
	const section = document.getElementById(sectionId);
	if (section) {
		section.scrollIntoView({ behavior: "smooth" });
	}
}

function formatCategoryName(categoryCode) {
	if (!categoryCode) return "Unknown Category";

	return categoryCode
		.split(".")
		.map((word) => capitalizeFirst(word))
		.join(" > ");
}

function capitalizeFirst(str) {
	if (!str) return "";
	return str.charAt(0).toUpperCase() + str.slice(1);
}

function calculateRecommendationScore(product) {
	const views = product.view_count || 0;
	const carts = product.cart_count || 0;
	const purchases = product.purchase_count || 0;

	// Simple scoring algorithm
	const score = Math.min(100, Math.round(views * 0.1 + carts * 2 + purchases * 5));

	return Math.max(10, score); // Minimum 10% to make it look realistic
}

// Search suggestion functionality
let searchTimeout;
searchInput.addEventListener("input", function () {
	clearTimeout(searchTimeout);
	searchTimeout = setTimeout(() => {
		if (this.value.length > 2) {
			showSearchSuggestions(this.value);
		} else {
			hideSearchSuggestions();
		}
	}, 300);
});

function showSearchSuggestions(query) {
	const suggestions = allProducts
		.filter((product) => {
			const text = `${product.category_code} ${product.brand}`.toLowerCase();
			return text.includes(query.toLowerCase());
		})
		.slice(0, 5);

	// Implementation for search suggestions dropdown
	// This would require additional HTML and CSS
}

function hideSearchSuggestions() {
	// Hide suggestions dropdown
}

// Real-time updates (if WebSocket is available)
function setupRealTimeUpdates() {
	// This would connect to a WebSocket for real-time analytics updates
	try {
		const ws = new WebSocket("ws://localhost:8080");

		ws.onmessage = function (event) {
			const data = JSON.parse(event.data);
			if (data.type === "analytics_update") {
				loadAllAnalytics();
			}
		};

		ws.onerror = function (error) {
			console.log("WebSocket connection failed:", error);
		};
	} catch (error) {
		console.log("WebSocket not available");
	}
}

// Initialize real-time updates
setTimeout(setupRealTimeUpdates, 2000);

// Auto-refresh analytics every 30 seconds
setInterval(() => {
	loadAllAnalytics();
}, 30000);

// Handle window resize for responsive design
window.addEventListener("resize", function () {
	// Adjust layout if needed
	const isMobile = window.innerWidth < 768;
	if (isMobile) {
		itemsPerPage = 6;
	} else {
		itemsPerPage = 12;
	}

	// Re-render current page
	renderProducts();
	updatePagination();
});

// Add smooth scroll behavior to all internal links
document.querySelectorAll('a[href^="#"]').forEach((anchor) => {
	anchor.addEventListener("click", function (e) {
		e.preventDefault();
		const target = document.querySelector(this.getAttribute("href"));
		if (target) {
			target.scrollIntoView({
				behavior: "smooth",
				block: "start",
			});
		}
	});
});

// Add loading animation to buttons
function addButtonLoading(button, originalText) {
	button.disabled = true;
	button.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Loading...';

	return function () {
		button.disabled = false;
		button.innerHTML = originalText;
	};
}

// Enhanced error handling
window.addEventListener("unhandledrejection", function (event) {
	console.error("Unhandled promise rejection:", event.reason);
	showError("An unexpected error occurred. Please refresh the page.");
});

// Performance monitoring
let pageLoadTime = performance.now();
window.addEventListener("load", function () {
	pageLoadTime = performance.now() - pageLoadTime;
	console.log(`Page loaded in ${pageLoadTime.toFixed(2)}ms`);
});

// async function loadRecommendations() {
// 	const userId = document.getElementById("userIdInput").value.trim();
// 	if (!userId) {
// 		alert("Please enter a User ID.");
// 		return;
// 	}

// 	const recGrid = document.getElementById("recommendationsGrid");
// 	recGrid.innerHTML = "<p>Loading recommendations...</p>";

// 	try {
// 		const response = await fetch(`${API_BASE_URL}/recommendations?user_id=${encodeURIComponent(userId)}&limit=10`);

// 		if (!response.ok) throw new Error("Failed to fetch recommendations");

// 		const data = await response.json();
// 		if (!data.recommendations || data.recommendations.length === 0) {
// 			recGrid.innerHTML = "<p>No recommendations found for this user.</p>";
// 			return;
// 		}

// 		recGrid.innerHTML = data.recommendations
// 			.map(
// 				(product) => `
//             <div class="product-card" onclick="showProductDetails('${product.product_id}')">
//                 <div class="product-image"><i class="fas fa-box"></i></div>
//                 <div class="product-info">
//                     <div class="product-title">Product ${product.product_id}</div>
//                     <div class="product-category">${formatCategoryName(product.category_code)}</div>
//                     <div class="product-brand">Brand: ${capitalizeFirst(product.brand)}</div>
//                     <div class="product-price">$${parseFloat(product.price).toFixed(2)}</div>
//                 </div>
//             </div>
//         `
// 			)
// 			.join("");
// 	} catch (error) {
// 		console.error("Error loading recommendations:", error);
// 		recGrid.innerHTML = "<p>Error loading recommendations.</p>";
// 	}
// }

function loadRecommendations() {
	const userId = document.getElementById("userIdInput").value.trim();
	const recGrid = document.getElementById("recommendationsGrid");

	if (userId !== "1" && userId !== "2") {
		recGrid.innerHTML = "<p>Only User ID 1 or 2 is supported for demo.</p>";
		return;
	}

	const products = [
		{ product_id: "1001", category_code: "construction.tools.light", brand: "apple", price: 1302.48 },
		{ product_id: "1002", category_code: "appliances.personal.massager", brand: "bosch", price: 313.52 },
		{ product_id: "1003", category_code: "apparel.trousers", brand: "nika", price: 101.68 },
		{ product_id: "1004", category_code: "apparel.costume", brand: "bourjois", price: 8.25 },
		{ product_id: "1005", category_code: "computers.peripherals.printer", brand: "lucente", price: 406.45 },
		{ product_id: "1006", category_code: "construction.tools.light", brand: "prestigio", price: 66.39 },
		{ product_id: "1007", category_code: "electronics.camera.video", brand: "sony", price: 565.78 },
		{ product_id: "1008", category_code: "construction.tools.light", brand: "samsung", price: 167.03 },
		{ product_id: "1009", category_code: "construction.tools.light", brand: "huawei", price: 205.67 },
		{ product_id: "1010", category_code: "construction.tools.light", brand: "apple", price: 912.5 },
	];

	// Shuffle if userId == "2"
	const toDisplay = userId === "2" ? shuffleArray([...products]) : products;

	recGrid.innerHTML = toDisplay
		.map(
			(product) => `
			<div class="product-card" onclick="showProductDetails('${product.product_id}')">
				<div class="product-image"><i class="fas fa-box"></i></div>
				<div class="product-info">
					<div class="product-title">Product ${product.product_id}</div>
					<div class="product-category">${formatCategoryName(product.category_code)}</div>
					<div class="product-brand">Brand: ${capitalizeFirst(product.brand)}</div>
					<div class="product-price">$${parseFloat(product.price).toFixed(2)}</div>
				</div>
			</div>
		`
		)
		.join("");
}

function shuffleArray(arr) {
	for (let i = arr.length - 1; i > 0; i--) {
		const j = Math.floor(Math.random() * (i + 1));
		[arr[i], arr[j]] = [arr[j], arr[i]];
	}
	return arr;
}

function formatCategoryName(cat) {
	return cat?.replace(/\./g, " > ") ?? "-";
}

function capitalizeFirst(word) {
	return word.charAt(0).toUpperCase() + word.slice(1);
}
