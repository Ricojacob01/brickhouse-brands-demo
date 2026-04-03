import { useEffect, useState, useMemo, useCallback, useRef } from "react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { UserAvatar } from "@/components/ui/UserAvatar";
import { Loader2, ChevronLeft, ChevronRight, Package, Store, User, Calendar, Edit3, AlertTriangle, Eye } from "lucide-react";
import { useOrderStore } from "@/store/useOrderStore";
import { useDarkModeStore } from "@/store/useDarkModeStore";
import { useDateStore } from "@/store/useDateStore";
import { Order } from "@/api/types";
import { ViewOrderDetailsModal } from "./ViewOrderDetailsModal";
import { getOrderExpiryDays } from "@/lib/config";
import { useCurrentDate, getDaysSince } from "@/utils/dateUtils";

const getStatusBadgeVariant = (status: string) => {
  switch (status) {
    case 'pending_review':
      return 'pending-review';
    case 'approved':
      return 'approved-order';
    case 'fulfilled':
      return 'fulfilled-order';
    case 'cancelled':
      return 'cancelled-order';
    default:
      return 'default';
  }
};

const getStatusText = (status: string) => {
  switch (status) {
    case 'pending_review':
      return 'Pendiente de Revision';
    case 'approved':
      return 'Aprobado';
    case 'fulfilled':
      return 'Completado';
    case 'cancelled':
      return 'Cancelado';
    default:
      return status;
  }
};

const formatDate = (date: string | Date) => {
  const dateObj = typeof date === 'string' ? new Date(date) : date;
  return dateObj.toLocaleDateString('es-MX', {
    month: 'short',
    day: 'numeric',
    year: 'numeric'
  });
};

// Helper function to check if expired SLA filter is active
const isExpiredSlaFilterActive = (orderFilters: any) => {
  return orderFilters.expiredSlaOnly === true;
};

// Helper function to calculate days since order creation (for display only)
const getDaysExpired = (orderDate: string | Date, currentDate: Date) => {
  const orderDateTime = typeof orderDate === 'string' ? new Date(orderDate) : orderDate;
  const diffTime = currentDate.getTime() - orderDateTime.getTime();
  const diffDays = diffTime / (1000 * 60 * 60 * 24); // Don't floor this
  return diffDays;
};

export const OrdersTable = () => {
  const {
    orders,
    currentPage,
    totalPages,
    totalItems,
    isBatchLoading,
    batchLoadingProgress,
    isLoading,
    error,
    batchFetchOrderData,
    setPage,
    setPageSize,
    filters,
    fetchOrders
  } = useOrderStore();

  // Get dark mode state
  const { isDarkMode } = useDarkModeStore();

  // Get date store for listening to changes
  const { isDateConfigured, configuredDate, resetCounter } = useDateStore();

  const [pageSize] = useState(10);
  const [isModifyModalOpen, setIsModifyModalOpen] = useState(false);
  const [selectedOrderForModify, setSelectedOrderForModify] = useState<Order | null>(null);

  const currentDate = useCurrentDate();

  // Track previous date configuration to detect changes
  const prevDateConfig = useRef({ isDateConfigured, configuredDate, resetCounter });

  // Auto-refresh when date configuration changes
  useEffect(() => {
    const currentDateConfig = { isDateConfigured, configuredDate, resetCounter };
    const prev = prevDateConfig.current;

    // Check if date configuration has changed
    const dateConfigChanged =
      prev.isDateConfigured !== currentDateConfig.isDateConfigured ||
      (prev.configuredDate && currentDateConfig.configuredDate &&
        prev.configuredDate.getTime() !== currentDateConfig.configuredDate?.getTime()) ||
      (!prev.configuredDate && currentDateConfig.configuredDate) ||
      (prev.configuredDate && !currentDateConfig.configuredDate) ||
      prev.resetCounter !== currentDateConfig.resetCounter; // Add reset counter check

    if (dateConfigChanged) {
      console.log('Date configuration changed, refreshing orders...');

      // If reset counter changed, this means date was reset to real-time
      // Clear any stale cached data first
      if (prev.resetCounter !== currentDateConfig.resetCounter) {
        console.log('Date was reset to real-time, refreshing orders after analytics...');
        // Longer delay to ensure OrdersTable loads AFTER OrderAnalyticsCards
        setTimeout(() => {
          batchFetchOrderData(filters, 1, pageSize, true);
        }, 150);
      } else {
        // Normal date change, refresh immediately
        batchFetchOrderData(filters, currentPage, pageSize, true);
      }
    }

    // Update the ref for next comparison
    prevDateConfig.current = currentDateConfig;
  }, [isDateConfigured, configuredDate, resetCounter, batchFetchOrderData, filters, currentPage, pageSize]);

  // Helper function to check if order can be modified
  const canModifyOrder = (order: Order) => {
    return order.order_status === 'pending_review' || order.order_status === 'approved';
  };

  // Helper function to check if order is view-only
  const isViewOnlyOrder = (order: Order) => {
    return order.order_status === 'fulfilled' || order.order_status === 'cancelled';
  };

  const handleOrderAction = (order: Order) => {
    setSelectedOrderForModify(order);
    setIsModifyModalOpen(true);
  };

  const handleCloseModal = () => {
    setIsModifyModalOpen(false);
    setSelectedOrderForModify(null);
  };

  useEffect(() => {
    // Set the page size in the store
    setPageSize(pageSize);
  }, [setPageSize, pageSize]);

  // Trigger initial orders load when component mounts (analytics loads separately)
  useEffect(() => {
    // Only trigger if we don't have data and aren't currently loading
    if (!isBatchLoading && orders.length === 0) {
      console.log('OrdersTable: Initial orders load on mount (analytics loads separately)');
      // Use fetchOrders instead of batchFetchOrderData to avoid loading analytics
      fetchOrders(filters, 1, pageSize);
    }
  }, [fetchOrders, filters, pageSize, isBatchLoading, orders.length]);

  const handlePageChange = (newPage: number) => {
    setPage(newPage);
  };

  // Use unified loading state for better UX
  const isCurrentlyLoading = isBatchLoading || isLoading;

  // Create progress indicator for batch loading
  const renderLoadingProgress = () => {
    if (!isBatchLoading) return null;

    const progress = {
      orders: !batchLoadingProgress.orders,
      statusSummary: !batchLoadingProgress.statusSummary,
      productPrefetch: !batchLoadingProgress.productPrefetch,
    };

    const completedSteps = Object.values(progress).filter(Boolean).length;
    const totalSteps = 3;
    const progressPercentage = (completedSteps / totalSteps) * 100;

    // Generate status text for what's currently loading
    const loadingSteps = [];
    if (batchLoadingProgress.orders) loadingSteps.push('pedidos');
    if (batchLoadingProgress.statusSummary) loadingSteps.push('analiticas');
    if (batchLoadingProgress.productPrefetch) loadingSteps.push('detalles de producto');

    const statusText = loadingSteps.length > 0
      ? `Cargando ${loadingSteps.join(', ')}...`
      : 'Finalizando...';

    return (
      <div className="relative">
        <div className="h-1 w-full bg-gray-200 dark:bg-gray-700 overflow-hidden">
          <div
            className="h-full bg-blue-500 transition-all duration-300 ease-out"
            style={{ width: `${progressPercentage}%` }}
          />
          {isDarkMode && (
            <div className="absolute inset-0 bg-gradient-to-r from-transparent via-blue-400/20 to-transparent animate-pulse" />
          )}
        </div>
        {/* Show loading status on mobile and desktop */}
        <div className={`text-xs px-3 py-1 ${isDarkMode ? 'text-gray-400' : 'text-gray-600'}`}>
          {statusText}
        </div>
      </div>
    );
  };

  const renderMobileCard = (order: Order) => (
    <div key={order.order_id} className={`rounded-lg border p-4 space-y-3 ${isDarkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200'
      }`}>
      <div className="flex justify-between items-start">
        <div>
          <p className={`font-medium ${isDarkMode ? 'text-white' : 'text-gray-900'}`}>{order.order_number}</p>
          <p className={`text-sm ${isDarkMode ? 'text-gray-300' : 'text-gray-600'}`}>{order.product_name}</p>
        </div>
        <div className="flex flex-col items-end">
          <div className="flex items-center gap-1">
            <Badge variant={getStatusBadgeVariant(order.order_status)}>
              {order.order_status === 'pending_review' && getDaysExpired(order.order_date, currentDate) > getOrderExpiryDays() && (
                <AlertTriangle className={`h-3 w-3 mr-1 ${isDarkMode ? 'text-red-400' : 'text-red-600'}`} />
              )}
              {getStatusText(order.order_status)}
            </Badge>
          </div>
          {order.order_status === 'pending_review' && getDaysExpired(order.order_date, currentDate) > getOrderExpiryDays() && (
            <div className={`text-xs mt-1 ${isDarkMode ? 'text-red-400' : 'text-red-600'}`}>
              {Math.max(0, Math.ceil(getDaysExpired(order.order_date, currentDate) - getOrderExpiryDays()))} dias vencidos
            </div>
          )}
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3 text-sm">
        <div className="flex items-center gap-2">
          <Package className={`h-4 w-4 ${isDarkMode ? 'text-gray-400' : 'text-gray-400'}`} />
          <span className={isDarkMode ? 'text-gray-300' : 'text-gray-700'}>{order.quantity_cases} cajas</span>
        </div>
        <div className="flex items-center gap-2">
          <Store className={`h-4 w-4 ${isDarkMode ? 'text-gray-400' : 'text-gray-400'}`} />
          <span className={isDarkMode ? 'text-gray-300' : 'text-gray-700'}>{order.to_store_name}</span>
        </div>
        <div className="flex items-center gap-2">
          <UserAvatar
            avatarUrl={order.requester_avatar_url}
            firstName={order.requester_name?.split(' ')[0]}
            lastName={order.requester_name?.split(' ')[1]}
            size="md"
          />
          <span className={isDarkMode ? 'text-gray-300' : 'text-gray-700'}>{order.requester_name}</span>
        </div>
        <div className="flex items-center gap-2">
          <Calendar className={`h-4 w-4 ${isDarkMode ? 'text-gray-400' : 'text-gray-400'}`} />
          <span className={isDarkMode ? 'text-gray-300' : 'text-gray-700'}>{formatDate(order.order_date)}</span>
        </div>
      </div>
    </div >
  );

  const renderSkeletonRow = () => (
    <tr className={`border-b ${isDarkMode ? 'border-gray-700' : 'border-gray-100'}`}>
      <td className="py-3 px-4">
        <div className={`h-4 rounded animate-pulse w-20 ${isDarkMode ? 'bg-gray-700' : 'bg-gray-200'
          }`}></div>
      </td>
      <td className="py-3 px-4">
        <div className="space-y-2">
          <div className={`h-4 rounded animate-pulse w-32 ${isDarkMode ? 'bg-gray-700' : 'bg-gray-200'
            }`}></div>
          <div className={`h-3 rounded animate-pulse w-24 ${isDarkMode ? 'bg-gray-700' : 'bg-gray-200'
            }`}></div>
        </div>
      </td>
      <td className="py-3 px-4">
        <div className={`h-4 rounded animate-pulse w-16 ${isDarkMode ? 'bg-gray-700' : 'bg-gray-200'
          }`}></div>
      </td>
      <td className="py-3 px-4">
        <div className="space-y-2">
          <div className={`h-4 rounded animate-pulse w-28 ${isDarkMode ? 'bg-gray-700' : 'bg-gray-200'
            }`}></div>
          <div className={`h-3 rounded animate-pulse w-20 ${isDarkMode ? 'bg-gray-700' : 'bg-gray-200'
            }`}></div>
        </div>
      </td>
      <td className="py-3 px-4">
        <div className={`h-4 rounded animate-pulse w-24 ${isDarkMode ? 'bg-gray-700' : 'bg-gray-200'
          }`}></div>
      </td>
      <td className="py-3 px-4">
        <div className={`h-4 rounded animate-pulse w-20 ${isDarkMode ? 'bg-gray-700' : 'bg-gray-200'
          }`}></div>
      </td>
      <td className="py-3 px-4">
        <div className={`h-6 rounded animate-pulse w-16 ${isDarkMode ? 'bg-gray-700' : 'bg-gray-200'
          }`}></div>
      </td>
      <td className="py-3 px-4 text-center">
        <div className="flex justify-center">
          <div className={`h-6 rounded animate-pulse w-20 ${isDarkMode ? 'bg-gray-700' : 'bg-gray-200'
            }`}></div>
        </div>
      </td>
      <td className="py-3 px-4 text-center">
        <div className="flex justify-center">
          <div className={`h-6 rounded animate-pulse w-16 ${isDarkMode ? 'bg-gray-700' : 'bg-gray-200'
            }`}></div>
        </div>
      </td>
    </tr>
  );

  const renderEmptyRow = () => (
    <tr className="">
      <td className="py-3 px-4"></td>
      <td className="py-3 px-4"></td>
      <td className="py-3 px-4"></td>
      <td className="py-3 px-4"></td>
      <td className="py-3 px-4"></td>
      <td className="py-3 px-4"></td>
      <td className="py-3 px-4 text-center"></td>
      <td className="py-3 px-4 text-center"></td>
    </tr>
  );

  // Create array of rows to ensure consistent count
  const getTableRows = () => {
    const rows = [];

    if (isCurrentlyLoading) {
      // Show skeleton rows while loading
      for (let i = 0; i < pageSize; i++) {
        rows.push(<div key={`skeleton-${i}`}>{renderSkeletonRow()}</div>);
      }
    } else {
      // Show actual order rows
      orders.forEach((order) => {
        rows.push(
          <tr key={order.order_id} className={`border-b ${isDarkMode ? 'border-gray-700 hover:bg-gray-700' : 'border-gray-100 hover:bg-gray-50'
            }`}>
            <td className="py-3 px-4">
              <span className={`font-medium ${isDarkMode ? 'text-blue-400' : 'text-blue-600'}`}>{order.order_number}</span>
            </td>
            <td className="py-3 px-4">
              <div>
                <p className={`font-medium ${isDarkMode ? 'text-white' : 'text-gray-900'}`}>{order.product_name}</p>
                <p className={`text-sm ${isDarkMode ? 'text-gray-400' : 'text-gray-600'}`}>{order.brand} • {order.category}</p>
              </div>
            </td>
            <td className="py-3 px-4">
              <span className={`font-medium ${isDarkMode ? 'text-white' : 'text-gray-900'}`}>{order.quantity_cases}</span>
              <span className={`ml-1 ${isDarkMode ? 'text-gray-400' : 'text-gray-600'}`}>cajas</span>
            </td>
            <td className="py-3 px-4">
              <div>
                <p className={`font-medium ${isDarkMode ? 'text-white' : 'text-gray-900'}`}>{order.to_store_name}</p>
                <p className={`text-sm ${isDarkMode ? 'text-gray-400' : 'text-gray-600'}`}>{order.to_store_region}</p>
              </div>
            </td>
            <td className="py-3 px-4">
              <div className="flex items-center gap-2">
                <UserAvatar
                  avatarUrl={order.requester_avatar_url}
                  firstName={order.requester_name?.split(' ')[0]}
                  lastName={order.requester_name?.split(' ')[1]}
                  size="md"
                />
                <span className={isDarkMode ? 'text-white' : 'text-gray-900'}>{order.requester_name}</span>
              </div>
            </td>
            <td className="py-3 px-4">
              <span className={isDarkMode ? 'text-gray-300' : 'text-gray-600'}>{formatDate(order.order_date)}</span>
            </td>
            <td className="py-3 px-4 text-center">
              <div className="flex flex-col items-center justify-center">
                <div className="flex items-center gap-1">
                  <Badge variant={getStatusBadgeVariant(order.order_status)} className="min-w-[100px] justify-center">
                    {order.order_status === 'pending_review' && getDaysExpired(order.order_date, currentDate) > getOrderExpiryDays() && (
                      <AlertTriangle className={`h-3 w-3 mr-1 ${isDarkMode ? 'text-red-400' : 'text-red-600'}`} />
                    )}
                    {getStatusText(order.order_status)}
                  </Badge>
                </div>
                {order.order_status === 'pending_review' && getDaysExpired(order.order_date, currentDate) > getOrderExpiryDays() && (
                  <div className={`text-xs mt-1 ${isDarkMode ? 'text-red-400' : 'text-red-600'}`}>
                    {Math.max(0, Math.ceil(getDaysExpired(order.order_date, currentDate) - getOrderExpiryDays()))} dias vencidos
                  </div>
                )}
              </div>
            </td>
            <td className="py-3 px-4 text-center">
              <div className="flex justify-center">
                {canModifyOrder(order) ? (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => handleOrderAction(order)}
                    className={`${isDarkMode
                      ? 'bg-gray-700 border-blue-500 text-blue-400 hover:bg-gray-600 hover:text-white'
                      : 'text-blue-600 hover:text-blue-700 hover:bg-blue-50'
                      }`}
                  >
                    <Edit3 className="h-4 w-4 mr-1" />
                    Modificar
                  </Button>
                ) : isViewOnlyOrder(order) ? (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => handleOrderAction(order)}
                    className={`${isDarkMode
                      ? 'bg-gray-700 border-blue-500 text-blue-400 hover:bg-gray-600 hover:text-white'
                      : 'text-blue-600 hover:text-blue-700 hover:bg-blue-50'
                      }`}
                  >
                    <Eye className="h-4 w-4 mr-1" />
                    Ver
                  </Button>
                ) : (
                  <span className={`text-sm ${isDarkMode ? 'text-gray-500' : 'text-gray-400'}`}>-</span>
                )}
              </div>
            </td>
          </tr>
        );
      });

      // Fill remaining rows with empty space to maintain consistent height
      const remainingRows = pageSize - orders.length;
      for (let i = 0; i < remainingRows; i++) {
        rows.push(<div key={`empty-${i}`}>{renderEmptyRow()}</div>);
      }
    }

    return rows;
  };

  if (error) {
    return (
      <div className={`text-center py-8 ${isDarkMode ? 'text-red-400' : 'text-red-600'}`}>
        <p className="font-medium">Error cargando pedidos</p>
        <p className={`text-sm mt-1 ${isDarkMode ? 'text-gray-400' : 'text-gray-600'}`}>{error}</p>
      </div>
    );
  }

  return (
    <div className={`rounded-lg border ${isDarkMode ? 'border-gray-700 bg-gray-800' : 'border-gray-200 bg-white'}`}>
      {/* Unified progress indicator */}
      {renderLoadingProgress()}

      {/* Mobile view */}
      <div className="lg:hidden">
        <div className="p-4 space-y-4">
          {isCurrentlyLoading ? (
            // Mobile skeleton loading
            Array.from({ length: pageSize }).map((_, index) => (
              <div
                key={`mobile-skeleton-${index}`}
                className={`rounded-lg border p-4 space-y-3 animate-pulse ${isDarkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200'
                  }`}
              >
                <div className="flex justify-between items-start">
                  <div className="space-y-2">
                    <div className={`h-4 w-24 rounded ${isDarkMode ? 'bg-gray-700' : 'bg-gray-200'}`}></div>
                    <div className={`h-3 w-32 rounded ${isDarkMode ? 'bg-gray-700' : 'bg-gray-200'}`}></div>
                  </div>
                  <div className={`h-6 w-20 rounded ${isDarkMode ? 'bg-gray-700' : 'bg-gray-200'}`}></div>
                </div>
                <div className="grid grid-cols-2 gap-3">
                  <div className={`h-4 w-16 rounded ${isDarkMode ? 'bg-gray-700' : 'bg-gray-200'}`}></div>
                  <div className={`h-4 w-20 rounded ${isDarkMode ? 'bg-gray-700' : 'bg-gray-200'}`}></div>
                  <div className={`h-4 w-24 rounded ${isDarkMode ? 'bg-gray-700' : 'bg-gray-200'}`}></div>
                  <div className={`h-4 w-18 rounded ${isDarkMode ? 'bg-gray-700' : 'bg-gray-200'}`}></div>
                </div>
              </div>
            ))
          ) : orders.length === 0 ? (
            <div className={`text-center py-8 ${isDarkMode ? 'text-gray-400' : 'text-gray-600'}`}>
              <Package className={`mx-auto h-12 w-12 mb-4 ${isDarkMode ? 'text-gray-600' : 'text-gray-400'}`} />
              <p className="font-medium">No se encontraron pedidos</p>
              <p className="text-sm mt-1">Intenta ajustar tus filtros o criterios de busqueda</p>
            </div>
          ) : (
            orders.map(renderMobileCard)
          )}
        </div>
      </div>

      {/* Desktop Table - Always show structure */}
      <div className="hidden md:block overflow-x-auto relative">
        {/* Loading overlay for desktop */}
        {isCurrentlyLoading && (
          <div className={`absolute inset-0 bg-opacity-75 flex items-center justify-center z-10 ${isDarkMode ? 'bg-gray-800' : 'bg-white'
            }`}>
            <div className="flex items-center">
              <Loader2 className={`h-6 w-6 animate-spin ${isDarkMode ? 'text-gray-400' : 'text-gray-400'}`} />
              <span className={`ml-2 ${isDarkMode ? 'text-gray-300' : 'text-gray-600'}`}>Cargando...</span>
            </div>
          </div>
        )}

        <table className="w-full">
          <thead>
            <tr className={`border-b ${isDarkMode ? 'border-gray-700' : 'border-gray-200'}`}>
              <th className={`text-left py-3 px-4 font-medium ${isDarkMode ? 'text-white' : 'text-gray-900'}`}>Pedido #</th>
              <th className={`text-left py-3 px-4 font-medium ${isDarkMode ? 'text-white' : 'text-gray-900'}`}>Producto</th>
              <th className={`text-left py-3 px-4 font-medium ${isDarkMode ? 'text-white' : 'text-gray-900'}`}>Cantidad</th>
              <th className={`text-left py-3 px-4 font-medium ${isDarkMode ? 'text-white' : 'text-gray-900'}`}>Tienda Destino</th>
              <th className={`text-left py-3 px-4 font-medium ${isDarkMode ? 'text-white' : 'text-gray-900'}`}>Solicitado Por</th>
              <th className={`text-left py-3 px-4 font-medium ${isDarkMode ? 'text-white' : 'text-gray-900'}`}>Fecha de Pedido</th>
              <th className={`text-left py-3 px-4 font-medium ${isDarkMode ? 'text-white' : 'text-gray-900'}`}>Estado</th>
              <th className={`text-left py-3 px-4 font-medium ${isDarkMode ? 'text-white' : 'text-gray-900'}`}>Acciones</th>
            </tr>
          </thead>
          <tbody>
            {getTableRows()}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className={`flex items-center justify-between mt-6 pt-4 px-6 pb-4 border-t ${isDarkMode ? 'border-gray-700' : 'border-gray-200'
          }`}>
          <div className={`flex items-center gap-4 text-sm ${isDarkMode ? 'text-gray-400' : 'text-gray-600'
            }`}>
            <span>Pagina {currentPage} de {totalPages}</span>
            <span>•</span>
            <span>{totalItems} pedidos totales</span>
          </div>
          <div className="flex gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => handlePageChange(currentPage - 1)}
              disabled={currentPage === 1 || isCurrentlyLoading}
              className={isDarkMode ? 'bg-gray-700 border-blue-500 text-blue-400 hover:bg-gray-600 hover:text-white' : ''}
            >
              <ChevronLeft className="h-4 w-4" />
              Anterior
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={() => handlePageChange(currentPage + 1)}
              disabled={currentPage === totalPages || isCurrentlyLoading}
              className={isDarkMode ? 'bg-gray-700 border-blue-500 text-blue-400 hover:bg-gray-600 hover:text-white' : ''}
            >
              Siguiente
              <ChevronRight className="h-4 w-4" />
            </Button>
          </div>
        </div>
      )}

      {/* Empty state - only show when not loading and no orders */}
      {!isCurrentlyLoading && orders.length === 0 && (
        <div className="text-center py-12 hidden md:block">
          <Package className={`h-12 w-12 mx-auto mb-4 ${isDarkMode ? 'text-gray-500' : 'text-gray-400'}`} />
          <p className={isDarkMode ? 'text-gray-400' : 'text-gray-600'}>No se encontraron pedidos</p>
        </div>
      )}

      {/* Order Details Modal */}
      <ViewOrderDetailsModal
        isOpen={isModifyModalOpen}
        onClose={handleCloseModal}
        order={selectedOrderForModify}
      />
    </div>
  );
};